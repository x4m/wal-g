package postgres

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	pgerrors "github.com/wal-g/wal-g/internal/databases/postgres/errors"
	"github.com/wal-g/wal-g/internal/ioextensions"
)

// Only numeric main-fork segments in the shared default tablespace participate.
// In particular SLRUs, local compute files, FSM/VM and pg_control are full files.
func polarDeltaPageFile(name string, size int64) bool {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) != 4 || parts[0] != "polar_shared_data" || parts[1] != "base" ||
		size <= 0 || size > RelFileSizeBound || size%DatabasePageSize != 0 {
		return false
	}
	for _, c := range parts[2] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return parts[2] != "" && pagedFilenameRegexp.MatchString(parts[3])
}

func validatePolarDeltaBase(parentName string, parent BackupSentinelDto, childName string,
	version int, systemID *uint64, start LSN) error {
	bad := func(reason string) error {
		return fmt.Errorf("unsafe PolarDB delta base %q: %s; take a new --full backup", parentName, reason)
	}
	if parent.BackupStartLSN == nil || parent.BackupFinishLSN == nil ||
		*parent.BackupStartLSN >= start || *parent.BackupFinishLSN > start {
		return bad("missing or inconsistent LSN bounds")
	}
	if parent.SystemIdentifier == nil || systemID == nil || *parent.SystemIdentifier != *systemID || parent.PgVersion != version {
		return bad("database identity or PostgreSQL version differs")
	}
	if parent.FilesMetadataDisabled {
		return bad("file metadata is disabled")
	}
	if parent.IncrementFrom != nil && !parent.PolarDBFullScanDelta {
		return bad("parent delta was not made by the PolarDB full-scan implementation")
	}
	pt, err := ParseTimelineFromBackupName(parentName)
	if err != nil {
		return bad("invalid parent timeline")
	}
	ct, err := ParseTimelineFromBackupName(childName)
	if err != nil || pt != ct {
		return bad("cross-timeline deltas are not supported")
	}
	return nil
}

// Use the ordinary PostgreSQL wi1 delta format. Two bounded sequential passes
// avoid a local staging file and work with either PFSD client without Seek.
// The first selects pages by the parent's START LSN; the second emits pages.
// File growth after stat is excluded; concurrent EOF is padded just as for a
// hot full backup. Only EOF is padded, never a transport or disk error.
func readPolarIncrement(ctx context.Context, open func(context.Context) (io.ReadCloser, error),
	size int64, lsn LSN) (io.ReadCloser, int64, error) {
	if size <= 0 || size > RelFileSizeBound || size%DatabasePageSize != 0 {
		return nil, 0, fmt.Errorf("invalid PolarDB incremental file size %d", size)
	}
	first, err := open(ctx)
	if err != nil {
		return nil, 0, err
	}
	selector := &IncrementalPageReader{Lsn: lsn}
	page := make([]byte, DatabasePageSize)
	r := polarBoundedPageSource(ctx, first, size)
	for block := uint32(0); int64(block)*DatabasePageSize < size; block++ {
		if _, err = io.ReadFull(r, page); err != nil {
			break
		}
		if !selector.SelectNewValidPage(page, block) {
			// Match the generic page reader's full-file fallback.
			err = pgerrors.NewInvalidBlockError(block)
			break
		}
	}
	closeErr := first.Close()
	if closeErr != nil {
		return nil, 0, closeErr
	}
	if err != nil {
		return nil, 0, err
	}
	var header bytes.Buffer
	header.Write(IncrementFileHeader)
	_ = binary.Write(&header, binary.LittleEndian, uint64(size))
	selector.WriteDiffMapToHeader(&header)
	packedSize := int64(header.Len()) + int64(len(selector.Blocks))*DatabasePageSize
	if len(selector.Blocks) == 0 {
		return io.NopCloser(bytes.NewReader(header.Bytes())), packedSize, nil
	}
	second, err := open(ctx)
	if err != nil {
		return nil, 0, err
	}
	pages := &polarSelectedPages{source: polarBoundedPageSource(ctx, second, size), blocks: selector.Blocks}
	return &ioextensions.ReadCascadeCloser{Reader: io.MultiReader(&header, pages), Closer: second}, packedSize, nil
}

func polarBoundedPageSource(ctx context.Context, source io.Reader, size int64) io.Reader {
	return bufio.NewReaderSize(&polarRestoreReader{ctx: ctx, reader: io.LimitReader(io.MultiReader(
		source, &ioextensions.ZeroReader{}), size)}, 1<<20)
}

type polarSelectedPages struct {
	source   io.Reader
	blocks   []uint32
	position int64
	pending  []byte
}

func (r *polarSelectedPages) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) == 0 {
		if len(r.blocks) == 0 {
			return 0, io.EOF
		}
		offset := int64(r.blocks[0]) * DatabasePageSize
		if _, err := io.CopyN(io.Discard, r.source, offset-r.position); err != nil {
			return 0, err
		}
		r.pending = make([]byte, DatabasePageSize)
		if _, err := io.ReadFull(r.source, r.pending); err != nil {
			r.pending = nil
			return 0, err
		}
		r.position = offset + DatabasePageSize
		r.blocks = r.blocks[1:]
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

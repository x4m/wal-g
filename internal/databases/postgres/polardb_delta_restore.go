package postgres

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sync"

	"github.com/wal-g/wal-g/internal"
)

type polarRestorePlan struct {
	name                   string
	files                  map[string]internal.BackupFileDescription
	concurrent, sequential []internal.ReaderMaker
}

// Resolve and validate the entire chain before touching either destination.
// Propagate only inherited files to ancestors, so dropped relations and stale
// local files cannot reappear. Keep the regular PostgreSQL parent selection.
func planPolarRestore(ctx context.Context, backup Backup) ([]polarRestorePlan, error) {
	var plans []polarRestorePlan
	var wanted map[string]bool
	var child *BackupSentinelDto
	var childName string
	visited := make(map[string]bool)
	for {
		if visited[backup.Name] {
			return nil, fmt.Errorf("cycle in PolarDB delta chain at %q", backup.Name)
		}
		visited[backup.Name] = true
		s, meta, err := backup.GetSentinelAndFilesMetadata(ctx)
		if err != nil {
			return nil, err
		}
		if s.TablespaceSpec != nil && !s.TablespaceSpec.empty() {
			return nil, fmt.Errorf("streaming PolarDB restore does not yet support tablespaces")
		}
		if child != nil {
			if err = validatePolarDeltaBase(backup.Name, s, childName, child.PgVersion, child.SystemIdentifier, *child.BackupStartLSN); err != nil {
				return nil, err
			}
			if *child.IncrementFromLSN != *s.BackupStartLSN {
				return nil, fmt.Errorf("PolarDB delta base LSN mismatch")
			}
		}
		incremental := s.IncrementFrom != nil
		if incremental && (!s.PolarDBFullScanDelta || s.IncrementFromLSN == nil || s.BackupStartLSN == nil ||
			s.IncrementFullName == nil || s.IncrementCount == nil || *s.IncrementCount < 1 || s.FilesMetadataDisabled || len(meta.Files) == 0) {
			return nil, fmt.Errorf("unsupported or incomplete PolarDB delta metadata for %q", backup.Name)
		}
		if !incremental && (s.IncrementFromLSN != nil || s.IncrementCount != nil || s.IncrementFullName != nil || s.IncrementFromChkpNum != nil) {
			return nil, fmt.Errorf("incomplete PolarDB delta metadata for %q", backup.Name)
		}
		plan := polarRestorePlan{name: backup.Name}
		if incremental || wanted != nil {
			plan.files = make(map[string]internal.BackupFileDescription)
			for raw, desc := range meta.Files {
				name, err := polarRestoreArchivePath(raw)
				if err != nil {
					return nil, err
				}
				if wanted == nil || wanted[name] {
					plan.files[name] = desc
				}
			}
			for name := range wanted {
				if _, ok := plan.files[name]; !ok {
					return nil, fmt.Errorf("PolarDB delta parent %q has no file %q", backup.Name, name)
				}
			}
		}
		plan.concurrent, plan.sequential, err = (FilesToExtractProviderImpl{}).Get(ctx, backup, nil, false)
		if err != nil {
			return nil, err
		}
		if len(plan.sequential) == 0 {
			return nil, newPgControlNotFoundError()
		}
		plans = append(plans, plan)
		if !incremental {
			for _, desc := range plan.files {
				if desc.IsIncremented || desc.IsSkipped {
					return nil, fmt.Errorf("full PolarDB base contains inherited files")
				}
			}
			return plans, nil
		}
		wanted = make(map[string]bool)
		for name, desc := range plan.files {
			if desc.IsSkipped && desc.IsIncremented {
				return nil, fmt.Errorf("conflicting delta flags for %q", name)
			}
			if desc.IsSkipped || desc.IsIncremented {
				wanted[name] = true
			}
		}
		child, childName = &s, backup.Name
		backup, err = NewBackupInStorage(ctx, backup.Folder, *s.IncrementFrom, backup.GetStorageName())
		if err != nil {
			return nil, err
		}
	}
}

func (p *polarDBRestoreInterpreter) checkRequiredFiles() error {
	for name, desc := range p.plan.files {
		if desc.IsSkipped {
			continue
		}
		if _, ok := p.seen.Load(name); !ok {
			return fmt.Errorf("PolarDB backup %q is missing required file %q", p.plan.name, name)
		}
	}
	return nil
}

// Merge sequentially into a fresh temporary PFS file. This needs no local
// staging or random-write SDK extension, and a tar retry is idempotent: blocks
// supplied by the delta replace the same positions on every attempt.
func (p *polarDBRestoreInterpreter) writeIncrement(name string, delta io.Reader, archiveSize int64) error {
	var header [16]byte
	if _, err := io.ReadFull(delta, header[:]); err != nil {
		return err
	}
	if !bytes.Equal(header[:4], IncrementFileHeader) {
		return newInvalidIncrementFileHeaderError()
	}
	size := binary.LittleEndian.Uint64(header[4:12])
	count := binary.LittleEndian.Uint32(header[12:16])
	if size > math.MaxInt64 || !polarDeltaPageFile("polar_shared_data/"+name, int64(size)) ||
		uint64(count) > size/uint64(DatabasePageSize) ||
		archiveSize != 16+int64(count)*(4+DatabasePageSize) {
		return fmt.Errorf("invalid PolarDB delta size or block count for %q", name)
	}
	// A large relation may have a large block map. Do not allocate from the
	// untrusted count alone: consume and validate bounded chunks first. A
	// truncated archive claiming billions of entries must fail without an OOM.
	var blocks []uint32
	var chunk [4096]uint32
	for left := count; left > 0; {
		n := min(left, uint32(len(chunk)))
		if err := binary.Read(delta, binary.LittleEndian, chunk[:n]); err != nil {
			return err
		}
		for _, b := range chunk[:n] {
			if uint64(b) >= size/uint64(DatabasePageSize) || (len(blocks) > 0 && b <= blocks[len(blocks)-1]) {
				return fmt.Errorf("invalid PolarDB delta block list for %q", name)
			}
			blocks = append(blocks, b)
		}
		left -= n
	}
	base, err := p.fs.Open(p.ctx, path.Join(p.root, name))
	if err != nil {
		return fmt.Errorf("open PolarDB delta base %q: %w", name, err)
	}
	r := &polarMergePages{base: bufio.NewReaderSize(&polarRestoreReader{ctx: p.ctx, reader: base}, 1<<20),
		delta: delta, blocks: blocks, remaining: int64(size)}
	// Release the inherited file before replacing its name, including on SDKs
	// that cannot replace an open inode. A close failure must prevent publish.
	closeBase := sync.OnceValue(base.Close)
	err = p.writeFile(name, r, int64(size), closeBase)
	return errors.Join(err, closeBase())
}

type polarMergePages struct {
	base, delta io.Reader
	blocks      []uint32
	block       uint32
	remaining   int64
	pending     []byte
	page        []byte
}

func (r *polarMergePages) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) == 0 {
		if r.remaining == 0 {
			return 0, io.EOF
		}
		if r.page == nil {
			r.page = make([]byte, DatabasePageSize)
		}
		page := r.page
		_, baseErr := io.ReadFull(r.base, page)
		changed := len(r.blocks) > 0 && r.blocks[0] == r.block
		if baseErr != nil && (!changed || !errors.Is(baseErr, io.EOF)) {
			return 0, fmt.Errorf("read inherited PolarDB page %d: %w", r.block, baseErr)
		}
		if changed {
			if _, err := io.ReadFull(r.delta, page); err != nil {
				return 0, err
			}
			r.blocks = r.blocks[1:]
		}
		r.pending = page
		r.block++
		r.remaining -= DatabasePageSize
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

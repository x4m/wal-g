package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const polarRestoreTemporaryDir = ".wal-g-restore"
const polarRestoreControl = ".wal-g-pg_control"

// Implementations use PFSD directly; no shared files are staged on local disk.
// The factory must check that the destination is empty before returning.
type polarDBRestoreFS interface {
	Mkdir(context.Context, string) error
	MkdirAll(context.Context, string) error
	Create(context.Context, string) (io.WriteCloser, error)
	Open(context.Context, string) (io.ReadCloser, error)
	Rename(context.Context, string, string) error
	Remove(context.Context, string) error
	Close() error
}

func GetPolarDBFetcher(pgData, root string) internal.Fetcher {
	return func(ctx context.Context, _ storage.Folder, source internal.Backup) {
		err := fetchPolarDBBackup(ctx, ToPgBackup(source), pgData, root, openPolarDBRestoreFS)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch PolarDB backup: %v\n", err)
	}
}

func fetchPolarDBBackup(ctx context.Context, backup Backup, pgData, root string,
	open func(context.Context, string) (polarDBRestoreFS, error),
) (err error) {
	root, err = polarRestoreRoot(root)
	if err != nil {
		return err
	}
	plans, err := planPolarRestore(ctx, backup)
	if err != nil {
		return err
	}
	pgData = utility.ResolveSymlink(pgData)
	if err = checkDBDirectoryForUnwrap(pgData, BackupSentinelDto{}, FilesMetadataDto{}); err != nil {
		return err
	}
	fs, err := open(ctx, root)
	if err != nil {
		return fmt.Errorf("mount PolarDB restore destination %q: %w", root, err)
	}
	interpreter := newPolarDBRestoreInterpreter(ctx, fs, root, pgData)
	// The generic extractor may return on cancellation before its workers.
	// Drain active writers and refuse late callbacks before unmounting PFSD.
	defer func() { interpreter.stop(); err = errors.Join(err, fs.Close()) }()
	// Exclusive directory creation also arbitrates simultaneous restores that
	// both observed an empty root. A failed restore keeps this claim in place.
	if err = fs.Mkdir(ctx, path.Join(root, polarRestoreTemporaryDir)); err != nil {
		return fmt.Errorf("create PolarDB restore temporary directory: %w", err)
	}
	for i := len(plans) - 1; i >= 0; i-- {
		// Each layer has its own lifecycle: late extraction callbacks must not
		// observe another layer's metadata. Only the newest layer has control.
		layer := newPolarDBRestoreInterpreter(ctx, fs, root, pgData)
		layer.plan = &plans[i]
		layer.latest = i == 0
		if len(plans[i].concurrent) > 0 {
			err = internal.ExtractAll(ctx, layer, plans[i].concurrent)
		}
		if err == nil {
			err = internal.ExtractAll(ctx, layer, plans[i].sequential)
		}
		layer.stop()
		if err != nil {
			return err
		}
		if err = layer.checkRequiredFiles(); err != nil {
			return err
		}
		if i == 0 {
			interpreter.control = layer.control
		}
	}
	interpreter.stop()
	if err = interpreter.finish(); err != nil {
		return err
	}
	tracelog.InfoLogger.Println("PolarDB backup restored directly through PFSD without local shared-data staging")
	return nil
}

func polarRestoreRoot(root string) (string, error) {
	root = strings.TrimSuffix(root, "/")
	if !strings.HasPrefix(root, "/") || path.Clean(root) != root ||
		len(strings.Split(strings.TrimPrefix(root, "/"), "/")) < 2 || strings.ContainsAny(root, "\\\x00") {
		return "", fmt.Errorf("PolarDB restore root must be a canonical /<device>/<directory> path, got %q", root)
	}
	return root, nil
}

type polarDBRestoreInterpreter struct {
	ctx      context.Context
	fs       polarDBRestoreFS
	root     string
	local    *FileTarInterpreter
	sequence atomic.Uint64
	mu       sync.Mutex
	control  []byte
	active   sync.WaitGroup
	stopped  bool
	plan     *polarRestorePlan
	latest   bool
	seen     sync.Map
}

func newPolarDBRestoreInterpreter(ctx context.Context, fs polarDBRestoreFS, root, pgData string) *polarDBRestoreInterpreter {
	return &polarDBRestoreInterpreter{ctx: ctx, fs: fs, root: root,
		local: NewFileTarInterpreter(pgData, BackupSentinelDto{}, FilesMetadataDto{}, nil, false)}
}

// WAL-G archives conventionally prefix datadir-relative names with one slash.
// Reject traversal rather than cleaning it into a different destination.
func polarRestoreArchivePath(name string) (string, error) {
	name = strings.TrimSuffix(strings.TrimPrefix(name, "/"), "/")
	if name == "" || name == "." {
		return ".", nil
	}
	if strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\\x00") || path.Clean(name) != name ||
		name == ".." || strings.HasPrefix(name, "../") {
		return "", fmt.Errorf("unsafe PolarDB archive path %q", name)
	}
	return name, nil
}

func (p *polarDBRestoreInterpreter) Interpret(reader io.Reader, header *tar.Header) (err error) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return fmt.Errorf("PolarDB restore is closed")
	}
	p.active.Add(1)
	p.mu.Unlock()
	defer p.active.Done()
	if err := p.ctx.Err(); err != nil {
		return err
	}
	name, err := polarRestoreArchivePath(header.Name)
	if err != nil {
		return err
	}
	if header.Typeflag != tar.TypeDir && header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
		return fmt.Errorf("unsupported PolarDB archive entry %q (type %d); links and special files are not supported", header.Name, header.Typeflag)
	}
	if name == "." && header.Typeflag != tar.TypeDir {
		return fmt.Errorf("invalid archive root entry")
	}
	if header.Size < 0 {
		return fmt.Errorf("negative size for %q", header.Name)
	}
	var increment bool
	if p.plan != nil {
		archiveName := name
		defer func() {
			if err == nil {
				p.seen.Store(archiveName, true)
			}
		}()
		if header.Typeflag == tar.TypeDir {
			if !p.latest {
				return nil
			}
		} else {
			desc, wanted := p.plan.files[name]
			utilityFile := name == "backup_label" || name == "tablespace_map" || name == "polar_shared_data/global/pg_control"
			if utilityFile && !p.latest {
				return nil
			}
			if p.plan.files != nil && !(utilityFile && p.latest) && (!wanted || desc.IsSkipped) {
				return nil
			}
			increment = wanted && desc.IsIncremented
		}
	}
	if name != "polar_shared_data" && !strings.HasPrefix(name, "polar_shared_data/") {
		if increment {
			return fmt.Errorf("unsupported local PolarDB increment %q", name)
		}
		// No links are accepted, and the local destination was checked empty.
		copyHeader := *header
		copyHeader.Name = name
		return p.local.Interpret(reader, &copyHeader)
	}
	name = strings.TrimPrefix(strings.TrimPrefix(name, "polar_shared_data"), "/")
	if name == polarRestoreTemporaryDir || strings.HasPrefix(name, polarRestoreTemporaryDir+"/") ||
		name == polarRestoreControl || strings.HasPrefix(name, polarRestoreControl+"/") {
		return fmt.Errorf("archive entry uses reserved restore path %q", name)
	}
	if header.Typeflag == tar.TypeDir {
		return p.fs.MkdirAll(p.ctx, path.Join(p.root, name))
	}
	if name == "" {
		return fmt.Errorf("shared-data root is not a directory")
	}
	if name == "global/pg_control" {
		if increment {
			return fmt.Errorf("pg_control cannot be incremental")
		}
		if header.Size != pgControlSize {
			return fmt.Errorf("invalid PolarDB pg_control size %d", header.Size)
		}
		data := make([]byte, pgControlSize)
		if _, err = io.ReadFull(reader, data); err != nil {
			return err
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.control != nil && !bytes.Equal(p.control, data) {
			return fmt.Errorf("conflicting PolarDB pg_control entries")
		}
		p.control = data
		return nil
	}
	if increment {
		return p.writeIncrement(name, reader, header.Size)
	}
	return p.writeFile(name, reader, header.Size)
}

func (p *polarDBRestoreInterpreter) stop() {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
	p.active.Wait()
}

// ExtractAll may retry a complete tar after a partial download. Each attempt
// writes a new temporary file and replaces the destination only after close.
func (p *polarDBRestoreInterpreter) writeFile(name string, reader io.Reader, size int64, beforePublish ...func() error) error {
	destination := path.Join(p.root, name)
	if err := p.fs.MkdirAll(p.ctx, path.Dir(destination)); err != nil {
		return err
	}
	temporary := path.Join(p.root, polarRestoreTemporaryDir, fmt.Sprint(p.sequence.Add(1)))
	file, err := p.fs.Create(p.ctx, temporary)
	if err != nil {
		return fmt.Errorf("create PolarDB restore file %q: %w", name, err)
	}
	// Bound memory by the number of extraction workers, not database size.
	n, copyErr := io.CopyBuffer(file, io.LimitReader(&polarRestoreReader{ctx: p.ctx, reader: reader}, size), make([]byte, 1<<20))
	closeErr := file.Close()
	if copyErr == nil && n != size {
		copyErr = io.ErrUnexpectedEOF
	}
	if err = errors.Join(copyErr, closeErr); err == nil {
		err = p.ctx.Err()
	}
	if err == nil {
		for _, closeSource := range beforePublish {
			if err = closeSource(); err != nil {
				break
			}
		}
	}
	if err == nil {
		err = p.fs.Rename(p.ctx, temporary, destination)
	}
	if err != nil {
		_ = p.fs.Remove(p.ctx, temporary)
		return fmt.Errorf("restore PolarDB file %q: %w", name, err)
	}
	return nil
}

type polarRestoreReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *polarRestoreReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(b)
}

func (p *polarDBRestoreInterpreter) finish() error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.control == nil {
		return fmt.Errorf("backup has no PolarDB shared pg_control; refusing to complete restore")
	}
	if err := p.writeFile(polarRestoreControl, bytes.NewReader(p.control), int64(len(p.control))); err != nil {
		return err
	}
	if err := p.fs.Remove(p.ctx, path.Join(p.root, polarRestoreTemporaryDir)); err != nil {
		return err
	}
	if err := p.fs.MkdirAll(p.ctx, path.Join(p.root, "global")); err != nil {
		return err
	}
	return p.fs.Rename(p.ctx, path.Join(p.root, polarRestoreControl), path.Join(p.root, "global/pg_control"))
}

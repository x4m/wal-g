package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	storagefs "github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// A separate filesystem represents PFSD; pgData must never receive shared data.
type testPolarRestoreFS struct {
	root         string
	mu           sync.Mutex
	published    []string
	closed       bool
	closeFileErr error
	renameErr    error
}

func (f *testPolarRestoreFS) disk(name string) string {
	return filepath.Join(f.root, strings.TrimPrefix(name, "/vdb/restore/"))
}
func (f *testPolarRestoreFS) Mkdir(_ context.Context, name string) error {
	return os.Mkdir(f.disk(name), 0700)
}
func (f *testPolarRestoreFS) MkdirAll(_ context.Context, name string) error {
	if name == "/vdb/restore" {
		return nil
	}
	return os.MkdirAll(f.disk(name), 0700)
}
func (f *testPolarRestoreFS) Create(_ context.Context, name string) (io.WriteCloser, error) {
	file, err := os.OpenFile(f.disk(name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &testPolarRestoreFile{File: file, closeErr: f.closeFileErr}, nil
}
func (f *testPolarRestoreFS) Open(_ context.Context, name string) (io.ReadCloser, error) {
	return os.Open(f.disk(name))
}

type testPolarRestoreFile struct {
	*os.File
	closeErr error
}

func (f *testPolarRestoreFile) Close() error { return errors.Join(f.File.Close(), f.closeErr) }
func (f *testPolarRestoreFS) Rename(_ context.Context, from, to string) error {
	if f.renameErr != nil {
		return f.renameErr
	}
	if err := os.Rename(f.disk(from), f.disk(to)); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, to)
	return nil
}
func (f *testPolarRestoreFS) Remove(_ context.Context, name string) error {
	return os.Remove(f.disk(name))
}
func (f *testPolarRestoreFS) Close() error { f.closed = true; return nil }

func newTestPolarRestore(t *testing.T) (*polarDBRestoreInterpreter, *testPolarRestoreFS, string) {
	t.Helper()
	f := &testPolarRestoreFS{root: t.TempDir()}
	require.NoError(t, f.Mkdir(t.Context(), "/vdb/restore/"+polarRestoreTemporaryDir))
	local := t.TempDir()
	return newPolarDBRestoreInterpreter(t.Context(), f, "/vdb/restore", local), f, local
}

func polarTestHeader(name, contents string) *tar.Header {
	return &tar.Header{Name: name, Size: int64(len(contents)), Mode: 0600, Typeflag: tar.TypeReg}
}

func TestPolarRestoreRoutesAndPublishesControlLast(t *testing.T) {
	p, f, local := newTestPolarRestore(t)
	control := strings.Repeat("c", pgControlSize)
	require.NoError(t, p.Interpret(strings.NewReader(control), polarTestHeader(PolarDBDirectPgControlPath, control)))
	require.NoFileExists(t, f.disk("/vdb/restore/global/pg_control"))
	for _, name := range []string{"/PG_VERSION", "/backup_label", "/polar_shared_data/base/1/42"} {
		require.NoError(t, p.Interpret(strings.NewReader("data"), polarTestHeader(name, "data")))
	}
	require.NoDirExists(t, filepath.Join(local, "polar_shared_data"))
	require.FileExists(t, filepath.Join(local, "PG_VERSION"))
	got, err := os.ReadFile(f.disk("/vdb/restore/base/1/42"))
	require.NoError(t, err)
	require.Equal(t, "data", string(got))
	require.NoError(t, p.finish())
	require.Equal(t, "/vdb/restore/global/pg_control", f.published[len(f.published)-1])
	require.NoDirExists(t, filepath.Join(f.root, polarRestoreTemporaryDir))
}

func TestPolarRestoreRejectsUnsafeEntries(t *testing.T) {
	for _, name := range []string{"../escape", "/polar_shared_data/../escape", "/polar_shared_data/base/../../escape", "//absolute", "bad\\name", "/polar_shared_data/.wal-g-restore/1", "/polar_shared_data/.wal-g-pg_control"} {
		t.Run(name, func(t *testing.T) {
			p, _, _ := newTestPolarRestore(t)
			require.Error(t, p.Interpret(strings.NewReader("x"), polarTestHeader(name, "x")))
		})
	}
	for _, kind := range []byte{tar.TypeSymlink, tar.TypeLink, tar.TypeFifo, tar.TypeChar} {
		p, _, _ := newTestPolarRestore(t)
		require.Error(t, p.Interpret(strings.NewReader(""), &tar.Header{Name: "/polar_shared_data/base/link", Typeflag: kind, Linkname: "/outside"}))
	}
}

func TestPolarRestoreFailuresDoNotPublishFile(t *testing.T) {
	for _, failure := range []string{"short", "read", "close", "rename", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			p, f, _ := newTestPolarRestore(t)
			var reader io.Reader = strings.NewReader("data")
			switch failure {
			case "short":
				reader = strings.NewReader("x")
			case "read":
				reader = iotest.ErrReader(io.ErrUnexpectedEOF)
			case "close":
				f.closeFileErr = io.ErrClosedPipe
			case "rename":
				f.renameErr = io.ErrClosedPipe
			case "cancel":
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				p.ctx = ctx
			}
			require.Error(t, p.Interpret(reader, polarTestHeader("/polar_shared_data/base/1/42", "data")))
			require.NoFileExists(t, f.disk("/vdb/restore/base/1/42"))
			require.NoFileExists(t, f.disk("/vdb/restore/global/pg_control"))
		})
	}
}

func TestPolarRestoreRetryAndConcurrentFiles(t *testing.T) {
	p, f, _ := newTestPolarRestore(t)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("/polar_shared_data/base/1/%d", i)
			errs <- p.Interpret(strings.NewReader("data"), polarTestHeader(name, "data"))
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	// Replaying a tar after a download failure must replace a complete file.
	require.NoError(t, p.Interpret(strings.NewReader("retry"), polarTestHeader("/polar_shared_data/base/1/0", "retry")))
	data, err := os.ReadFile(f.disk("/vdb/restore/base/1/0"))
	require.NoError(t, err)
	require.Equal(t, "retry", string(data))
	require.ErrorContains(t, p.finish(), "no PolarDB shared pg_control")
}

func TestPolarRestoreRootValidation(t *testing.T) {
	for _, root := range []string{"/", "/vdb", "/vdb/", "/vdb/../x", "/vdb//x", "vdb/x", "/vdb/./x"} {
		_, err := polarRestoreRoot(root)
		require.Error(t, err, root)
	}
	root, err := polarRestoreRoot("/vdb/new/")
	require.NoError(t, err)
	require.Equal(t, "/vdb/new", root)
}

func makePolarRestoreBackup(t *testing.T) Backup {
	t.Helper()
	backup, err := NewBackup(storagefs.NewFolder(t.TempDir(), ""), "base_000000010000000000000001")
	require.NoError(t, err)
	backup.SentinelDto = &BackupSentinelDto{}
	backup.FilesMetadataDto = &FilesMetadataDto{}
	for _, part := range []struct{ name, path, data string }{
		{"part_001.tar", "/polar_shared_data/base/1/42", "relation"},
		{"part_002.tar", "/PG_VERSION", "17"},
		{"backup_label.tar", "/backup_label", "label"},
		{"pg_control.tar", PolarDBDirectPgControlPath, strings.Repeat("c", pgControlSize)},
	} {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		require.NoError(t, tw.WriteHeader(polarTestHeader(part.path, part.data)))
		_, err = io.WriteString(tw, part.data)
		require.NoError(t, err)
		require.NoError(t, tw.Close())
		require.NoError(t, backup.GetTarPartitionFolder().PutObject(t.Context(), part.name, &buf))
	}
	return backup
}

func TestPolarRestoreFetchStreamsArchives(t *testing.T) {
	backup := makePolarRestoreBackup(t)
	local := t.TempDir()
	f := &testPolarRestoreFS{root: t.TempDir()}
	err := fetchPolarDBBackup(t.Context(), backup, local, "/vdb/restore", func(context.Context, string) (polarDBRestoreFS, error) { return f, nil })
	require.NoError(t, err)
	require.True(t, f.closed)
	require.NoDirExists(t, filepath.Join(local, "polar_shared_data"))
	require.FileExists(t, filepath.Join(local, "backup_label"))
	require.FileExists(t, f.disk("/vdb/restore/base/1/42"))
	require.FileExists(t, f.disk("/vdb/restore/global/pg_control"))
}

func TestPolarRestoreRejectsBeforeMount(t *testing.T) {
	for _, kind := range []string{"delta", "tablespace", "local nonempty"} {
		t.Run(kind, func(t *testing.T) {
			backup := makePolarRestoreBackup(t)
			local := t.TempDir()
			switch kind {
			case "delta":
				parent := "base_old"
				lsn := LSN(1)
				backup.SentinelDto.IncrementFrom = &parent
				backup.SentinelDto.IncrementFromLSN = &lsn
			case "tablespace":
				spec := NewTablespaceSpec(local)
				spec.addTablespace("123", "/outside")
				backup.SentinelDto.TablespaceSpec = &spec
			case "local nonempty":
				require.NoError(t, os.WriteFile(filepath.Join(local, "keep"), []byte("original"), 0600))
			}
			called := false
			err := fetchPolarDBBackup(t.Context(), backup, local, "/vdb/restore", func(context.Context, string) (polarDBRestoreFS, error) {
				called = true
				return nil, errors.New("unexpected mount")
			})
			require.Error(t, err)
			require.False(t, called)
		})
	}
}

func TestPolarRestoreMountFailureLeavesLocalEmpty(t *testing.T) {
	local := t.TempDir()
	err := fetchPolarDBBackup(t.Context(), makePolarRestoreBackup(t), local, "/vdb/restore", func(context.Context, string) (polarDBRestoreFS, error) { return nil, errors.New("mount refused") })
	require.ErrorContains(t, err, "mount refused")
	entries, err := os.ReadDir(local)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestPolarRestoreExistingClaimPreventsExtraction(t *testing.T) {
	_, f, local := newTestPolarRestore(t)
	err := fetchPolarDBBackup(t.Context(), makePolarRestoreBackup(t), local, "/vdb/restore",
		func(context.Context, string) (polarDBRestoreFS, error) { return f, nil })
	require.ErrorContains(t, err, "create PolarDB restore temporary directory")
	require.True(t, f.closed)
	entries, err := os.ReadDir(local)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.Empty(t, f.published)
	require.DirExists(t, f.disk("/vdb/restore/"+polarRestoreTemporaryDir))
}

var _ internal.TarInterpreter = (*polarDBRestoreInterpreter)(nil)

func TestPolarRestoreCorruptControlTarNeverPublishes(t *testing.T) {
	t.Setenv(conf.DownloadConcurrencySetting, "1")
	t.Setenv(conf.DownloadFileRetriesSetting, "0")
	backup := makePolarRestoreBackup(t)
	// Keep only the failing sequential archive to avoid the generic
	// extractor's backoff after a partially successful batch.
	require.NoError(t, backup.GetTarPartitionFolder().DeleteObjects(t.Context(), []storage.Object{
		storage.NewLocalObject("backup_label.tar", time.Time{}, 0),
	}))
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	control := strings.Repeat("c", pgControlSize)
	require.NoError(t, w.WriteHeader(polarTestHeader(PolarDBDirectPgControlPath, control)))
	_, err := io.WriteString(w, control)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	// pg_control is read successfully, but the next header is corrupt.
	data := append(append([]byte{}, buf.Bytes()[:buf.Len()-1024]...), bytes.Repeat([]byte{1}, 512)...)
	require.NoError(t, backup.GetTarPartitionFolder().PutObject(t.Context(), "pg_control.tar", bytes.NewReader(data)))
	f := &testPolarRestoreFS{root: t.TempDir()}
	err = fetchPolarDBBackup(t.Context(), backup, t.TempDir(), "/vdb/restore", func(context.Context, string) (polarDBRestoreFS, error) { return f, nil })
	require.Error(t, err)
	require.True(t, f.closed)
	require.NoFileExists(t, f.disk("/vdb/restore/global/pg_control"))
}

type polarRestoreBlockingReader struct {
	entered chan struct{}
	release chan struct{}
}

func (r *polarRestoreBlockingReader) Read(b []byte) (int, error) {
	close(r.entered)
	<-r.release
	return copy(b, "data"), io.EOF
}

func TestPolarRestoreStopDrainsActiveWrites(t *testing.T) {
	p, f, _ := newTestPolarRestore(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p.ctx = ctx
	reader := &polarRestoreBlockingReader{entered: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-reader.release:
		default:
			close(reader.release)
		}
	}()
	written := make(chan error, 1)
	go func() {
		written <- p.Interpret(reader, polarTestHeader("/polar_shared_data/base/1/42", "data"))
	}()
	<-reader.entered
	cancel()
	stopped := make(chan struct{})
	go func() { p.stop(); close(stopped) }()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.stopped
	}, time.Second, time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("stop returned while a writer was active")
	default:
	}
	require.ErrorContains(t, p.Interpret(strings.NewReader("data"), polarTestHeader("PG_VERSION", "data")), "closed")
	close(reader.release)
	require.ErrorIs(t, <-written, context.Canceled)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not finish after the writer exited")
	}
	require.NoFileExists(t, f.disk("/vdb/restore/base/1/42"))
}

func TestPolarRestoreControlValidation(t *testing.T) {
	p, _, _ := newTestPolarRestore(t)
	require.Error(t, p.Interpret(strings.NewReader("bad"), polarTestHeader(PolarDBDirectPgControlPath, "bad")))
	first := strings.Repeat("a", pgControlSize)
	second := strings.Repeat("b", pgControlSize)
	require.NoError(t, p.Interpret(strings.NewReader(first), polarTestHeader(PolarDBDirectPgControlPath, first)))
	require.NoError(t, p.Interpret(strings.NewReader(first), polarTestHeader(PolarDBDirectPgControlPath, first)))
	require.ErrorContains(t, p.Interpret(strings.NewReader(second), polarTestHeader(PolarDBDirectPgControlPath, second)), "conflicting")
}

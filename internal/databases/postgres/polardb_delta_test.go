package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	storagefs "github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func polarTestPage(lsn LSN, data byte) []byte {
	p := make([]byte, DatabasePageSize)
	binary.LittleEndian.PutUint32(p, uint32(uint64(lsn)>>32))
	binary.LittleEndian.PutUint32(p[4:], uint32(lsn))
	binary.LittleEndian.PutUint16(p[12:], 24)
	binary.LittleEndian.PutUint16(p[14:], uint16(DatabasePageSize))
	binary.LittleEndian.PutUint16(p[16:], uint16(DatabasePageSize))
	binary.LittleEndian.PutUint16(p[18:], uint16(DatabasePageSize)|4)
	p[100] = data
	return p
}

func polarTestIncrement(t *testing.T, data []byte, lsn LSN) []byte {
	t.Helper()
	r, size, err := readPolarIncrement(t.Context(), func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}, int64(len(data)), lsn)
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Len(t, out, int(size))
	return out
}

func TestPolarDeltaScanAndMerge(t *testing.T) {
	old := polarTestPage(5, 'a')
	for _, tc := range []struct {
		name   string
		pages  [][]byte
		blocks []uint32
	}{
		{"unchanged", [][]byte{old}, nil},
		{"equal start LSN", [][]byte{polarTestPage(10, 'b')}, []uint32{0}},
		{"changed and grown", [][]byte{old, polarTestPage(11, 'b')}, []uint32{1}},
		{"zero page", [][]byte{old, make([]byte, DatabasePageSize)}, []uint32{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Join(tc.pages, nil)
			delta := polarTestIncrement(t, data, 10)
			// Independent decoder: the ordinary PG restore consumes this format.
			pgFile := filepath.Join(t.TempDir(), "42")
			require.NoError(t, os.WriteFile(pgFile, append(append([]byte{}, old...), old...), 0600))
			require.NoError(t, ApplyFileIncrement(pgFile, bytes.NewReader(delta), false, false))
			pgData, err := os.ReadFile(pgFile)
			require.NoError(t, err)
			require.Equal(t, data, pgData)
			require.EqualValues(t, len(tc.blocks), binary.LittleEndian.Uint32(delta[12:16]))
			for i, b := range tc.blocks {
				require.Equal(t, b, binary.LittleEndian.Uint32(delta[16+4*i:]))
			}
			p, fs, _ := newTestPolarRestore(t)
			name := "base/1/42"
			// Longer base also exercises truncation without changed pages.
			require.NoError(t, p.writeFile(name, bytes.NewReader(append(append([]byte{}, old...), old...)), 2*DatabasePageSize))
			require.NoError(t, p.writeIncrement(name, bytes.NewReader(delta), int64(len(delta))))
			got, err := os.ReadFile(fs.disk("/vdb/restore/" + name))
			require.NoError(t, err)
			require.Equal(t, data, got)
			// Replaying the same tar is idempotent after publication.
			require.NoError(t, p.writeIncrement(name, bytes.NewReader(delta), int64(len(delta))))
		})
	}
}

func TestPolarDeltaGrowthEOFAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source []byte
		size   int64
	}{
		{"growth ignored", append(polarTestPage(11, 1), polarTestPage(12, 2)...), DatabasePageSize},
		{"truncate padded", nil, DatabasePageSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _, err := readPolarIncrement(t.Context(), func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(tc.source)), nil
			}, tc.size, 10)
			require.NoError(t, err)
			out, err := io.ReadAll(r)
			require.NoError(t, err)
			require.NoError(t, r.Close())
			require.Len(t, out, 20+int(DatabasePageSize))
		})
	}
	for _, failure := range []error{io.ErrUnexpectedEOF, os.ErrPermission, context.Canceled} {
		_, _, err := readPolarIncrement(t.Context(), func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(iotest.ErrReader(failure)), nil
		}, DatabasePageSize, 10)
		require.ErrorIs(t, err, failure)
	}
}

func TestPolarDeltaInvalidPageFallsBackThroughOpener(t *testing.T) {
	data := bytes.Repeat([]byte{255}, int(DatabasePageSize))
	cfi := &internal.ComposeFileInfo{Path: "/not/a/local/file", IsIncremented: true,
		Header: &tar.Header{Size: DatabasePageSize}, FileInfo: fakeDirectFileInfo{name: "42", size: DatabasePageSize},
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }}
	lsn := LSN(10)
	r, err := (&TarBallFilePackerImpl{incrementFromLsn: &lsn}).createFileReadCloser(t.Context(), cfi)
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.False(t, cfi.IsIncremented)
	require.Equal(t, data, got)
}

func TestPolarDeltaRejectsMalformedIncrement(t *testing.T) {
	valid := polarTestIncrement(t, polarTestPage(11, 1), 10)
	for _, kind := range []string{"magic", "size", "count", "block", "short payload", "missing base", "short base"} {
		t.Run(kind, func(t *testing.T) {
			p, fs, _ := newTestPolarRestore(t)
			data := append([]byte{}, valid...)
			base := polarTestPage(5, 9)
			if kind == "short base" {
				base = base[:1]
			}
			if kind != "missing base" {
				require.NoError(t, p.writeFile("base/1/42", bytes.NewReader(base), int64(len(base))))
			}
			switch kind {
			case "magic":
				data[0] = 0
			case "size":
				binary.LittleEndian.PutUint64(data[4:], 1<<63)
			case "count":
				binary.LittleEndian.PutUint32(data[12:], 1<<31)
			case "block":
				binary.LittleEndian.PutUint32(data[16:], 99)
			case "short payload":
				data = data[:len(data)-1]
			}
			require.Error(t, p.writeIncrement("base/1/42", bytes.NewReader(data), int64(len(valid))))
			require.NoFileExists(t, fs.disk("/vdb/restore/global/pg_control"))
			if kind != "missing base" {
				got, err := os.ReadFile(fs.disk("/vdb/restore/base/1/42"))
				require.NoError(t, err)
				require.Equal(t, base, got)
			}
		})
	}
}

func storePolarDeltaFixture(t *testing.T, folder storage.Folder, name string, start LSN, parent *Backup,
	files map[string][]byte, incremental map[string]bool) Backup {
	t.Helper()
	b, err := NewBackup(folder, name)
	require.NoError(t, err)
	finish, sysid := start+1, uint64(123)
	s := BackupSentinelDto{BackupStartLSN: &start, BackupFinishLSN: &finish, SystemIdentifier: &sysid, PgVersion: 170000}
	if parent != nil {
		count := 1
		full := parent.Name
		if parent.SentinelDto.IncrementCount != nil {
			count += *parent.SentinelDto.IncrementCount
			full = *parent.SentinelDto.IncrementFullName
		}
		s.IncrementFrom, s.IncrementFromLSN, s.IncrementCount, s.IncrementFullName = &parent.Name, parent.SentinelDto.BackupStartLSN, &count, &full
		s.PolarDBFullScanDelta = true
	}
	meta := FilesMetadataDto{Files: make(internal.BackupFileList)}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, data := range files {
		meta.Files["/"+name] = internal.BackupFileDescription{IsIncremented: incremental[name]}
		h := polarTestHeader("/"+name, string(data))
		require.NoError(t, tw.WriteHeader(h))
		_, err = tw.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, b.GetTarPartitionFolder().PutObject(t.Context(), "part_001.tar", &buf))
	for file, data := range map[string]string{"backup_label": name, "polar_shared_data/global/pg_control": strings.Repeat("c", pgControlSize)} {
		buf.Reset()
		tw = tar.NewWriter(&buf)
		require.NoError(t, tw.WriteHeader(polarTestHeader("/"+file, data)))
		_, err = io.WriteString(tw, data)
		require.NoError(t, err)
		require.NoError(t, tw.Close())
		require.NoError(t, b.GetTarPartitionFolder().PutObject(t.Context(), filepath.Base(file)+".tar", &buf))
	}
	require.NoError(t, b.UploadSentinel(t.Context(), s))
	require.NoError(t, internal.UploadDto(t.Context(), folder, meta, getFilesMetadataPath(name)))
	b.SentinelDto, b.FilesMetadataDto = &s, &meta
	return b
}

func TestPolarDeltaRestoreChain(t *testing.T) {
	t.Setenv(conf.DownloadFileRetriesSetting, "0")
	folder := storagefs.NewFolder(t.TempDir(), "")
	name := "polar_shared_data/base/1/42"
	old := polarTestPage(5, 'a')
	base := storePolarDeltaFixture(t, folder, "base_000000010000000000000001", 10, nil,
		map[string][]byte{name: old, "polar_shared_data/base/1/99": old, "old.conf": []byte("deleted"),
			"postgresql.conf": []byte("unchanged config")}, nil)
	changed := append(append([]byte{}, old...), polarTestPage(15, 'b')...)
	d1 := storePolarDeltaFixture(t, folder, "base_000000010000000000000002_D_000000010000000000000001", 20, &base,
		map[string][]byte{name: polarTestIncrement(t, changed, 10)}, map[string]bool{name: true})
	final := polarTestPage(25, 'c')
	d2 := storePolarDeltaFixture(t, folder, "base_000000010000000000000003_D_000000010000000000000002", 30, &d1,
		map[string][]byte{name: polarTestIncrement(t, final, 20), "PG_VERSION": []byte("17")}, map[string]bool{name: true})
	// An unchanged local file exists only in the full backup's tar. Both
	// deltas inherit it through metadata, just like PostgreSQL mtime skips.
	for _, b := range []Backup{d1, d2} {
		b.FilesMetadataDto.Files["/postgresql.conf"] = internal.BackupFileDescription{IsSkipped: true}
		require.NoError(t, internal.UploadDto(t.Context(), folder, b.FilesMetadataDto, getFilesMetadataPath(b.Name)))
	}
	fs := &testPolarRestoreFS{root: t.TempDir()}
	local := t.TempDir()
	require.NoError(t, fetchPolarDBBackup(t.Context(), d2, local, "/vdb/restore",
		func(context.Context, string) (polarDBRestoreFS, error) { return fs, nil }))
	got, err := os.ReadFile(fs.disk("/vdb/restore/base/1/42"))
	require.NoError(t, err)
	require.Equal(t, final, got)
	require.NoFileExists(t, fs.disk("/vdb/restore/base/1/99"))
	require.NoFileExists(t, filepath.Join(local, "old.conf"))
	require.NoDirExists(t, filepath.Join(local, "polar_shared_data"))
	config, err := os.ReadFile(filepath.Join(local, "postgresql.conf"))
	require.NoError(t, err)
	require.Equal(t, "unchanged config", string(config))
	label, err := os.ReadFile(filepath.Join(local, "backup_label"))
	require.NoError(t, err)
	require.Equal(t, d2.Name, string(label))
	require.Equal(t, "/vdb/restore/global/pg_control", fs.published[len(fs.published)-1])
	for _, p := range fs.published[:len(fs.published)-1] {
		require.NotEqual(t, "/vdb/restore/global/pg_control", p)
	}
	// Missing inherited file must fail before mounting.
	delete(base.FilesMetadataDto.Files, "/"+name)
	require.NoError(t, internal.UploadDto(t.Context(), folder, base.FilesMetadataDto, getFilesMetadataPath(base.Name)))
	err = fetchPolarDBBackup(t.Context(), d2, t.TempDir(), "/vdb/restore", func(context.Context, string) (polarDBRestoreFS, error) {
		t.Fatal("must reject missing parent metadata before mount")
		return nil, errors.New("unexpected mount")
	})
	require.ErrorContains(t, err, "has no file")
}

type polarRecordingComposer struct {
	internal.TarBallComposer
	added   []*internal.ComposeFileInfo
	skipped int
}

func (c *polarRecordingComposer) AddFile(info *internal.ComposeFileInfo) {
	c.added = append(c.added, info)
}
func (c *polarRecordingComposer) SkipFile(*tar.Header, os.FileInfo) { c.skipped++ }

func TestPolarDeltaMtimeAndOnlySharedPages(t *testing.T) {
	t.Setenv(PolarDBDirectDataPathEnv, "/vdb/source")
	lsn := LSN(10)
	for _, tc := range []struct {
		name                         string
		shared, incremental, skipped bool
	}{
		{"polar_shared_data/base/1/42", true, true, false},
		{"polar_shared_data/base/1/42.1", true, true, false},
		{"polar_shared_data/base/1/42_fsm", true, false, false},
		{"polar_shared_data/base/1/42_vm", true, false, false},
		{"polar_shared_data/global/42", true, false, false},
		{"polar_shared_data/pg_xact/0000", true, false, false},
		{"base/1/42", false, false, true},
		{"postgresql.conf", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, changed := range []bool{false, true} {
				base := internal.BackupFileList{"/" + tc.name: {}}
				if changed {
					base["/"+tc.name] = internal.BackupFileDescription{MTime: time.Unix(1, 0)}
				}
				b := NewBundle("/local", nil, "base", &lsn, base, false, 1<<30)
				c := &polarRecordingComposer{}
				b.TarBallComposer = c
				var open func(context.Context) (io.ReadCloser, error)
				if tc.shared {
					open = func(context.Context) (io.ReadCloser, error) {
						t.Fatal("unexpected read while scheduling")
						return nil, nil
					}
				}
				require.NoError(t, b.addToBundleWithOpener("/local/"+tc.name, fakeDirectFileInfo{name: filepath.Base(tc.name), size: DatabasePageSize}, open))
				if tc.skipped && !changed {
					require.Equal(t, 1, c.skipped)
					require.Empty(t, c.added)
					continue
				}
				require.Zero(t, c.skipped)
				require.Len(t, c.added, 1)
				require.Equal(t, tc.incremental, c.added[0].IsIncremented)
			}
		})
	}
}

func TestPolarDeltaBaseValidation(t *testing.T) {
	start, finish, sysid := LSN(10), LSN(20), uint64(123)
	base := BackupSentinelDto{BackupStartLSN: &start, BackupFinishLSN: &finish, SystemIdentifier: &sysid, PgVersion: 170000}
	parent, child := "base_000000010000000000000001", "base_000000010000000000000002"
	require.NoError(t, validatePolarDeltaBase(parent, base, child, 170000, &sysid, 30))
	for _, kind := range []string{"timeline", "identity", "version", "LSN", "metadata", "unmarked delta"} {
		t.Run(kind, func(t *testing.T) {
			s, childName := base, child
			switch kind {
			case "timeline":
				childName = "base_000000020000000000000002"
			case "identity":
				s.SystemIdentifier = nil
			case "version":
				s.PgVersion = 150000
			case "LSN":
				s.BackupFinishLSN = nil
			case "metadata":
				s.FilesMetadataDisabled = true
			case "unmarked delta":
				s.IncrementFrom = &parent
			}
			require.Error(t, validatePolarDeltaBase(parent, s, childName, 170000, &sysid, 30))
		})
	}
}

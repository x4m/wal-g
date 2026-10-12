package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

func TestPolarDeltaLargeFileEligibility(t *testing.T) {
	t.Setenv(PolarDBDirectDataPathEnv, "/vdb/source")
	name := "polar_shared_data/base/5/16479"
	maxSize := int64(math.MaxUint32) * DatabasePageSize
	for _, size := range []int64{1 << 30, 1<<30 + DatabasePageSize, 8 << 30, maxSize} {
		require.True(t, polarDeltaPageFile(name, size), "size=%d", size)
		lsn := LSN(10)
		bundle := NewBundle("/local", nil, "base", &lsn, internal.BackupFileList{"/" + name: {}}, false, 1<<30)
		composer := &polarRecordingComposer{}
		bundle.TarBallComposer = composer
		require.NoError(t, bundle.addToBundleWithOpener("/local/"+name, fakeDirectFileInfo{name: "16479", size: size},
			func(context.Context) (io.ReadCloser, error) {
				t.Fatal("unexpected read while scheduling")
				return nil, nil
			}))
		require.Len(t, composer.added, 1)
		require.True(t, composer.added[0].IsIncremented, "size=%d", size)
	}
	for _, size := range []int64{-1, 0, 1<<30 + 1, maxSize + DatabasePageSize, math.MaxInt64} {
		require.False(t, polarDeltaPageFile(name, size), "size=%d", size)
		_, _, err := readPolarIncrement(t.Context(), func(context.Context) (io.ReadCloser, error) {
			t.Fatal("invalid size must be rejected before opening")
			return nil, nil
		}, size, 10)
		require.Error(t, err)
	}
}

func TestPolarDeltaLargeTruncatedMap(t *testing.T) {
	p, _, _ := newTestPolarRestore(t)
	var header [16]byte
	copy(header[:4], IncrementFileHeader)
	binary.LittleEndian.PutUint64(header[4:12], uint64(math.MaxUint32)*uint64(DatabasePageSize))
	binary.LittleEndian.PutUint32(header[12:], math.MaxUint32)
	size := 16 + int64(math.MaxUint32)*(4+DatabasePageSize)
	require.ErrorIs(t, p.writeIncrement("base/5/16479", bytes.NewReader(header[:]), size), io.EOF)
}

func TestPolarDeltaMapChunkBoundary(t *testing.T) {
	const count = 4097
	for _, last := range []uint32{4096, 4095, 0, count} {
		t.Run(fmt.Sprint(last), func(t *testing.T) {
			data := make([]byte, 16+4*count)
			copy(data[:4], IncrementFileHeader)
			binary.LittleEndian.PutUint64(data[4:12], uint64(count)*uint64(DatabasePageSize))
			binary.LittleEndian.PutUint32(data[12:16], count)
			for b := uint32(0); b < count; b++ {
				binary.LittleEndian.PutUint32(data[16+4*b:], b)
			}
			binary.LittleEndian.PutUint32(data[len(data)-4:], last)
			p, _, _ := newTestPolarRestore(t)
			err := p.writeIncrement("base/5/16479", bytes.NewReader(data), 16+count*(4+DatabasePageSize))
			if last == count-1 {
				// A valid multi-chunk map reaches the (deliberately absent) base.
				require.ErrorContains(t, err, "open PolarDB delta base")
			} else {
				require.ErrorContains(t, err, "invalid PolarDB delta block list")
			}
		})
	}
}

// Generate real page bytes sequentially, without allocating a GiB-sized buffer
// or writing GiBs to the unit-test filesystem.
type polarLargeTestReader struct {
	size, pos int64
	old       []byte
	changed   map[uint32][]byte
}

func (r *polarLargeTestReader) Read(p []byte) (int, error) {
	n := 0
	for len(p) > 0 && r.pos < r.size {
		page := r.changed[uint32(r.pos/DatabasePageSize)]
		if page == nil {
			page = r.old
		}
		count := copy(p, page[r.pos%DatabasePageSize:])
		n += count
		r.pos += int64(count)
		p = p[count:]
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

type polarLargeTestWriter struct {
	want   io.Reader
	closed bool
	buf    [32768]byte
}

func (w *polarLargeTestWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		count := min(len(p), len(w.buf))
		if _, err := io.ReadFull(w.want, w.buf[:count]); err != nil {
			return n, err
		}
		if !bytes.Equal(p[:count], w.buf[:count]) {
			return n, fmt.Errorf("restored page contents differ")
		}
		n += count
		p = p[count:]
	}
	return n, nil
}

func (w *polarLargeTestWriter) Close() error {
	w.closed = true
	if n, err := w.want.Read(w.buf[:1]); n != 0 || err != io.EOF {
		return fmt.Errorf("restored file has incorrect length")
	}
	return nil
}

type polarLargeTestFS struct {
	*testPolarRestoreFS
	base   io.Reader
	writer *polarLargeTestWriter
	done   bool
}

func (f *polarLargeTestFS) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(f.base), nil
}
func (f *polarLargeTestFS) Create(context.Context, string) (io.WriteCloser, error) {
	return f.writer, nil
}
func (f *polarLargeTestFS) Rename(context.Context, string, string) error {
	if !f.writer.closed {
		return fmt.Errorf("publish before close")
	}
	f.done = true
	return nil
}

func TestPolarDeltaLargeFileRoundTrip(t *testing.T) {
	// Also cross the 32-bit byte-offset boundary, not just the old 1 GiB cap.
	const largeSize int64 = 5 << 30
	boundary := uint32(largeSize / DatabasePageSize)
	old := polarTestPage(5, 'a')
	type state struct {
		size    int64
		changed map[uint32][]byte
	}
	reader := func(s state) io.Reader {
		return &polarLargeTestReader{size: s.size, old: old, changed: s.changed}
	}
	base := state{size: largeSize + 2*DatabasePageSize}
	for _, tc := range []struct {
		name   string
		lsn    LSN
		next   state
		blocks []uint32
	}{
		{"unchanged", 10, base, nil},
		{"change past four GiB", 10, state{base.size, map[uint32][]byte{boundary + 1: polarTestPage(11, 'b')}}, []uint32{boundary + 1}},
		{"truncate", 20, state{largeSize + DatabasePageSize, nil}, nil},
		{"regrow", 30, state{largeSize + 3*DatabasePageSize, map[uint32][]byte{
			boundary + 1: polarTestPage(31, 'c'), boundary + 2: polarTestPage(31, 'd'),
		}}, []uint32{boundary + 1, boundary + 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, size, err := readPolarIncrement(t.Context(), func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(reader(tc.next)), nil
			}, tc.next.size, tc.lsn)
			require.NoError(t, err)
			delta, err := io.ReadAll(r)
			require.NoError(t, err)
			require.NoError(t, r.Close())
			require.EqualValues(t, 16+int64(len(tc.blocks))*(4+DatabasePageSize), size)
			require.EqualValues(t, size, len(delta))
			require.EqualValues(t, tc.next.size, binary.LittleEndian.Uint64(delta[4:12]))
			for i, block := range tc.blocks {
				require.Equal(t, block, binary.LittleEndian.Uint32(delta[16+4*i:]))
			}

			p, fs, _ := newTestPolarRestore(t)
			large := &polarLargeTestFS{testPolarRestoreFS: fs, base: reader(base), writer: &polarLargeTestWriter{want: reader(tc.next)}}
			p.fs = large
			require.NoError(t, p.writeIncrement("base/5/16479", bytes.NewReader(delta), size))
			require.True(t, large.done)

			// Independently check the existing PostgreSQL decoder's large offsets
			// and final length, using a sparse file (no full-size allocation).
			filename := filepath.Join(t.TempDir(), "16479")
			file, err := os.Create(filename)
			require.NoError(t, err)
			require.NoError(t, file.Truncate(base.size))
			require.NoError(t, file.Close())
			require.NoError(t, ApplyFileIncrement(filename, bytes.NewReader(delta), false, false))
			file, err = os.Open(filename)
			require.NoError(t, err)
			defer file.Close()
			info, err := file.Stat()
			require.NoError(t, err)
			require.Equal(t, tc.next.size, info.Size())
			page := make([]byte, DatabasePageSize)
			for _, block := range tc.blocks {
				_, err = file.ReadAt(page, int64(block)*DatabasePageSize)
				require.NoError(t, err)
				require.Equal(t, tc.next.changed[block], page)
			}
		})
		base = tc.next
	}
}

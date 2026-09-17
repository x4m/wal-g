package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

type polarDirectTestReader struct {
	io.Reader
	closed bool
}

func (r *polarDirectTestReader) Close() error { r.closed = true; return nil }

func TestPolarDBDirectReaderConcurrentSizeChange(t *testing.T) {
	for _, tc := range []struct{ name, contents, expected string }{
		{"unchanged", "abcd", "abcd"},
		{"grown after stat", "abcdef", "abcd"},
		{"truncated after stat", "ab", "ab\x00\x00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &polarDirectTestReader{Reader: strings.NewReader(tc.contents)}
			cfi := &internal.ComposeFileInfo{
				Header: &tar.Header{Name: "polar_shared_data/base/1/123", Size: 4, Mode: 0600},
				Open:   func(context.Context) (io.ReadCloser, error) { return source, nil },
			}
			reader, err := (&TarBallFilePackerImpl{}).createFileReadCloser(t.Context(), cfi)
			require.NoError(t, err)
			defer reader.Close()
			var packed bytes.Buffer
			tw := tar.NewWriter(&packed)
			require.NoError(t, tw.WriteHeader(cfi.Header))
			n, err := io.Copy(tw, reader)
			require.NoError(t, err)
			require.EqualValues(t, 4, n)
			require.NoError(t, reader.Close())
			require.True(t, source.closed)
			require.NoError(t, tw.Close())
			tr := tar.NewReader(&packed)
			_, err = tr.Next()
			require.NoError(t, err)
			data, err := io.ReadAll(tr)
			require.NoError(t, err)
			require.Equal(t, tc.expected, string(data))
		})
	}
}

func TestPolarDBDirectReaderPreservesIOError(t *testing.T) {
	source := &polarDirectTestReader{Reader: iotest.ErrReader(io.ErrUnexpectedEOF)}
	cfi := &internal.ComposeFileInfo{
		Header: &tar.Header{Size: 4},
		Open:   func(context.Context) (io.ReadCloser, error) { return source, nil },
	}
	reader, err := (&TarBallFilePackerImpl{}).createFileReadCloser(t.Context(), cfi)
	require.NoError(t, err)
	defer reader.Close()
	_, err = io.ReadAll(reader)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestPolarDBDirectReaderDeletedBeforeOpen(t *testing.T) {
	cfi := &internal.ComposeFileInfo{
		Path: "polar_shared_data/base/1/123", Header: &tar.Header{Size: 4},
		Open: func(context.Context) (io.ReadCloser, error) { return nil, os.ErrNotExist },
	}
	_, err := (&TarBallFilePackerImpl{}).createFileReadCloser(t.Context(), cfi)
	var missing internal.FileNotExistError
	require.ErrorAs(t, err, &missing)
}

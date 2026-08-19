package postgres

import (
	"archive/tar"
	"context"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

type fakeDirectFileInfo struct {
	name string
	size int64
}

func (info fakeDirectFileInfo) Name() string       { return info.name }
func (info fakeDirectFileInfo) Size() int64        { return info.size }
func (info fakeDirectFileInfo) Mode() fs.FileMode  { return 0o600 }
func (info fakeDirectFileInfo) ModTime() time.Time { return time.Time{} }
func (info fakeDirectFileInfo) IsDir() bool        { return false }
func (info fakeDirectFileInfo) Sys() any           { return nil }

func TestDirectFileOpenerDoesNotUseLocalPath(t *testing.T) {
	opened := false
	composeInfo := &internal.ComposeFileInfo{
		Path:     "/path/that/does/not/exist",
		Header:   &tar.Header{Size: 7},
		FileInfo: fakeDirectFileInfo{name: "123", size: 7},
		Open: func(_ context.Context) (io.ReadCloser, error) {
			opened = true
			return io.NopCloser(strings.NewReader("payload")), nil
		},
	}
	packer := NewTarBallFilePacker(nil, nil, &internal.NopBundleFiles{}, NewTarBallFilePackerOptions(false, false))
	reader, err := packer.createFileReadCloser(t.Context(), composeInfo)
	require.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.True(t, opened)
	assert.Equal(t, "payload", string(data))
}

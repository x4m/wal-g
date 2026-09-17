package postgres

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/utility"
)

func TestPolarDBBackupNameCollision(t *testing.T) {
	const name = "base_000000010000000000000001"
	for _, key := range []string{name + utility.SentinelSuffix, name + "/metadata.json", name + "/tar_partitions/part_001.tar.lz4"} {
		t.Run(key, func(t *testing.T) {
			ctx := context.Background()
			folder := fs.NewFolder(t.TempDir(), "")
			require.NoError(t, folder.PutObject(ctx, key, strings.NewReader("original")))
			err := checkPolarDBBackupNameAvailable(ctx, folder, name)
			require.ErrorContains(t, err, "refusing to overwrite")
			reader, err := folder.ReadObject(ctx, key)
			require.NoError(t, err)
			defer reader.Close()
			contents, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, "original", string(contents))
			require.NoError(t, checkPolarDBBackupNameAvailable(ctx, folder, "base_000000010000000000000002"))
		})
	}
}

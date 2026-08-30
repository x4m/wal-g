package postgres

import (
	"context"
	"io"
	"os"
	"path"
	"strings"
)

const PolarDBDirectDataPathEnv = "WALG_POLARDB_PFS_DATA_PATH"

type polarDBDirectSource interface {
	AddToBundle(context.Context, *Bundle, string) error
	Open(context.Context, string) (io.ReadCloser, error)
	Close() error
}

type polarDBDirectWalkStats struct {
	files         int64
	nonEmptyFiles int64
	bytes         int64
	hasPgControl  bool
}

func polarDBDirectDataPath() string { return os.Getenv(PolarDBDirectDataPathEnv) }

func polarDBWALDestination(root, walFileName string) string {
	return path.Join("/"+strings.TrimLeft(root, "/"), "pg_wal", path.Base(walFileName))
}

// RestorePolarDBSharedData moves the shared subtree extracted by backup-fetch
// to PFS when direct PolarDB mode is enabled.
func RestorePolarDBSharedData(ctx context.Context, pgData string) error {
	root := polarDBDirectDataPath()
	if root == "" {
		return nil
	}
	return restorePolarDBSharedDataPlatform(ctx, pgData, root)
}

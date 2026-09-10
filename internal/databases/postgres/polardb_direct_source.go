package postgres

import (
	"context"
	"io"
	"path"
	"strings"

	conf "github.com/wal-g/wal-g/internal/config"
)

const PolarDBDirectDataPathEnv = conf.PolarDBPFSDataPath

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

func polarDBDirectDataPath() string {
	value, _ := conf.GetSetting(PolarDBDirectDataPathEnv)
	return value
}

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

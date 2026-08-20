package postgres

import (
	"context"
	"io"
	"os"
)

const PolarDBDirectDataPathEnv = "WALG_POLARDB_PFS_DATA_PATH"

type polarDBDirectSource interface {
	AddToBundle(context.Context, *Bundle, string) error
	Open(context.Context, string) (io.ReadCloser, error)
	Close() error
}

func polarDBDirectDataPath() string { return os.Getenv(PolarDBDirectDataPathEnv) }

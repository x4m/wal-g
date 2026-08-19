package postgres

import (
	"context"
	"os"
)

const PolarDBDirectDataPathEnv = "WALG_POLARDB_PFS_DATA_PATH"

type polarDBDirectSource interface {
	AddToBundle(context.Context, *Bundle, string) error
	Close() error
}

func polarDBDirectDataPath() string { return os.Getenv(PolarDBDirectDataPathEnv) }

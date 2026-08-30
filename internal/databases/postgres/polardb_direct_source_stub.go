//go:build (!pfsnative && !pfs) || !linux || (pfs && !cgo) || (pfsnative && !amd64)

package postgres

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/internal"
)

func openPolarDBDirectSource(context.Context, string) (polarDBDirectSource, error) {
	return nil, fmt.Errorf("%s requires a Linux WAL-G build with CGO and -tags pfs, or linux/amd64 with -tags pfsnative", PolarDBDirectDataPathEnv)
}

func restorePolarDBSharedDataPlatform(context.Context, string, string) error {
	return fmt.Errorf("%s restore requires a Linux WAL-G build with CGO and -tags pfs", PolarDBDirectDataPathEnv)
}

func handlePolarDBWALFetch(context.Context, internal.StorageFolderReader, string) (bool, error) {
	return false, nil
}

//go:build !pfsnative || !linux || !amd64

package postgres

import (
	"context"
	"fmt"
)

func openPolarDBDirectSource(context.Context, string) (polarDBDirectSource, error) {
	return nil, fmt.Errorf("%s requires a linux/amd64 WAL-G build with -tags pfsnative", PolarDBDirectDataPathEnv)
}

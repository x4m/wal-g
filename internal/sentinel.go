package internal

import (
	"context"
	"io"
	"os"
)

// Sentinel is used to signal completion of a walked
// Directory.
type Sentinel struct {
	Info os.FileInfo
	Path string
	Open func(context.Context) (io.ReadCloser, error)
}

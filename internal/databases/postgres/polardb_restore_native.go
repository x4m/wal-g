//go:build pfsnative && !pfs && linux && amd64

package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/pkg/pfsnative"
)

type nativePolarDBRestoreFS struct{ client *pfsnative.Client }

func openPolarDBRestoreFS(ctx context.Context, root string) (polarDBRestoreFS, error) {
	client, root, err := openNativePFSClient(ctx, root, false)
	if err != nil {
		return nil, err
	}
	if _, err = client.Stat(ctx, root); err == nil {
		entries, readErr := client.ReadDir(ctx, root)
		err = readErr
		if err == nil && len(entries) != 0 {
			err = fmt.Errorf("PolarDB restore destination %q is not empty", root)
		}
	} else if errors.Is(err, syscall.ENOENT) {
		err = client.MkdirAll(ctx, root, 0o700)
	}
	if err != nil {
		return nil, errors.Join(err, client.Close())
	}
	return &nativePolarDBRestoreFS{client}, nil
}

func (f *nativePolarDBRestoreFS) MkdirAll(ctx context.Context, name string) error {
	return f.client.MkdirAll(ctx, name, 0o700)
}
func (f *nativePolarDBRestoreFS) Mkdir(ctx context.Context, name string) error {
	return f.client.Mkdir(ctx, name, 0o700)
}
func (f *nativePolarDBRestoreFS) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	file, err := f.client.OpenFile(ctx, name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return &nativePolarRestoreWriter{ctx: ctx, File: file}, nil
}
func (f *nativePolarDBRestoreFS) Rename(ctx context.Context, from, to string) error {
	return f.client.Rename(ctx, from, to)
}
func (f *nativePolarDBRestoreFS) Remove(ctx context.Context, name string) error {
	return f.client.Remove(ctx, name)
}
func (f *nativePolarDBRestoreFS) Close() error { return f.client.Close() }

type nativePolarRestoreWriter struct {
	ctx context.Context
	*pfsnative.File
}

func (w *nativePolarRestoreWriter) Write(b []byte) (int, error) { return w.File.WriteContext(w.ctx, b) }

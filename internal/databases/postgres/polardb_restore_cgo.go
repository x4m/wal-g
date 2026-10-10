//go:build pfs && !pfsnative && linux && cgo

package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/pkg/pfsclient"
)

type cgoPolarDBRestoreFS struct{ client *pfsclient.Client }

func openPolarDBRestoreFS(ctx context.Context, root string) (polarDBRestoreFS, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, root, err := openCGOPFSClient(root, false)
	if err != nil {
		return nil, err
	}
	if _, err = client.Stat(root); err == nil {
		entries, readErr := client.ReadDir(root)
		err = readErr
		if err == nil && len(entries) != 0 {
			err = fmt.Errorf("PolarDB restore destination %q is not empty", root)
		}
	} else if errors.Is(err, syscall.ENOENT) {
		err = client.MkdirAll(root)
	}
	if err != nil {
		return nil, errors.Join(err, client.Close())
	}
	return &cgoPolarDBRestoreFS{client}, nil
}

func (f *cgoPolarDBRestoreFS) MkdirAll(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.client.MkdirAll(name)
}
func (f *cgoPolarDBRestoreFS) Mkdir(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.client.Mkdir(name)
}
func (f *cgoPolarDBRestoreFS) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.client.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}
func (f *cgoPolarDBRestoreFS) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.client.OpenFile(name, os.O_RDONLY, 0)
}
func (f *cgoPolarDBRestoreFS) Rename(ctx context.Context, from, to string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.client.Rename(from, to)
}
func (f *cgoPolarDBRestoreFS) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.client.Remove(name)
}
func (f *cgoPolarDBRestoreFS) Close() error { return f.client.Close() }

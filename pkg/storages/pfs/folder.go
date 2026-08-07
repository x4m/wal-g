//go:build pfs && linux && cgo

package pfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"syscall"

	"github.com/wal-g/wal-g/internal/contextio"
	"github.com/wal-g/wal-g/pkg/pfsclient"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type Folder struct {
	client            *pfsclient.Client
	rootPath, subPath string
}

func (f *Folder) GetPath() string             { return f.subPath }
func (f *Folder) filePath(name string) string { return path.Join(f.rootPath, f.subPath, name) }

func (f *Folder) ListFolder(_ context.Context) ([]storage.Object, []storage.Folder, error) {
	dir := f.filePath("")
	entries, err := f.client.ReadDir(dir)
	if errors.Is(err, syscall.ENOENT) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("list PFS folder %q: %w", dir, err)
	}
	var objects []storage.Object
	var folders []storage.Folder
	for _, entry := range entries {
		if storage.HasTimestampRandomTmpSuffix(entry.Name) {
			continue
		}
		if entry.IsDir {
			folders = append(folders, &Folder{f.client, f.rootPath, path.Join(f.subPath, entry.Name) + "/"})
		} else {
			objects = append(objects, storage.NewLocalObject(entry.Name, entry.ModTime, entry.Size))
		}
	}
	return objects, folders, nil
}

func (f *Folder) Exists(_ context.Context, name string) (bool, error) {
	_, err := f.client.Stat(f.filePath(name))
	if errors.Is(err, syscall.ENOENT) {
		return false, nil
	}
	return err == nil, err
}

func (f *Folder) GetSubFolder(name string) storage.Folder {
	sub := &Folder{f.client, f.rootPath, path.Join(f.subPath, name)}
	_ = f.client.MkdirAll(sub.filePath(""))
	return sub
}

func (f *Folder) ReadObject(_ context.Context, name string) (io.ReadCloser, error) {
	file, err := f.client.OpenFile(f.filePath(name), syscall.O_RDONLY, 0)
	if errors.Is(err, syscall.ENOENT) {
		return nil, storage.NewObjectNotFoundError(name)
	}
	return file, err
}

func (f *Folder) PutObject(ctx context.Context, name string, content io.Reader) error {
	finalPath := f.filePath(name)
	if err := f.client.MkdirAll(path.Dir(finalPath)); err != nil {
		return err
	}
	tag, err := storage.NewTimestampRandomTag()
	if err != nil {
		return err
	}
	tmpPath := finalPath + tag
	file, err := f.client.OpenFile(tmpPath, syscall.O_CREAT|syscall.O_TRUNC|syscall.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	cleanup := func() { _ = file.Close(); _ = f.client.Remove(tmpPath) }
	reader := contextio.NewReader(ctx, content)
	buffer := make([]byte, 1024*1024)
	for {
		n, readErr := reader.Read(buffer)
		for written := 0; written < n; {
			wn, writeErr := file.Write(buffer[written:n])
			if writeErr != nil {
				cleanup()
				return writeErr
			}
			if wn == 0 {
				cleanup()
				return io.ErrShortWrite
			}
			written += wn
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			cleanup()
			return readErr
		}
	}
	if err := file.Close(); err != nil {
		_ = f.client.Remove(tmpPath)
		return err
	}
	if err := f.client.Rename(tmpPath, finalPath); err != nil {
		_ = f.client.Remove(tmpPath)
		return err
	}
	return nil
}

func (f *Folder) DeleteObjects(_ context.Context, objects []storage.Object) error {
	for _, object := range objects {
		if err := f.client.Remove(f.filePath(object.GetName())); err != nil && !errors.Is(err, syscall.ENOENT) {
			return err
		}
	}
	return nil
}

func (f *Folder) CopyObject(ctx context.Context, source, destination string) error {
	file, err := f.client.OpenFile(path.Join(f.rootPath, source), syscall.O_RDONLY, 0)
	if errors.Is(err, syscall.ENOENT) {
		return storage.NewObjectNotFoundError(source)
	}
	if err != nil {
		return err
	}
	defer file.Close()
	return f.PutObject(ctx, destination, file)
}

func (f *Folder) Validate(context.Context) error             { _, err := f.client.Stat(f.filePath("")); return err }
func (f *Folder) SetVersioningEnabled(context.Context, bool) {}
func (f *Folder) GetVersioningEnabled(context.Context) bool  { return false }

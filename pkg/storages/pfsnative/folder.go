//go:build pfsnative && linux && amd64

package pfsnative

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"syscall"

	"github.com/wal-g/wal-g/internal/contextio"
	client "github.com/wal-g/wal-g/pkg/pfsnative"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type Folder struct {
	client            *client.Client
	rootPath, subPath string
}

func (f *Folder) GetPath() string             { return f.subPath }
func (f *Folder) filePath(name string) string { return path.Join(f.rootPath, f.subPath, name) }

func (f *Folder) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	dir := f.filePath("")
	entries, err := f.client.ReadDir(ctx, dir)
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
		if entry.IsDir() {
			folders = append(folders, &Folder{f.client, f.rootPath, path.Join(f.subPath, entry.Name) + "/"})
		} else {
			objects = append(objects, storage.NewLocalObject(entry.Name, entry.ModTime(), entry.Size()))
		}
	}
	return objects, folders, nil
}

func (f *Folder) Exists(ctx context.Context, name string) (bool, error) {
	_, err := f.client.Stat(ctx, f.filePath(name))
	if errors.Is(err, syscall.ENOENT) {
		return false, nil
	}
	return err == nil, err
}

func (f *Folder) GetSubFolder(name string) storage.Folder {
	return &Folder{client: f.client, rootPath: f.rootPath, subPath: path.Join(f.subPath, name)}
}

func (f *Folder) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	file, err := f.client.Open(ctx, f.filePath(name))
	if errors.Is(err, syscall.ENOENT) {
		return nil, storage.NewObjectNotFoundError(name)
	}
	return file, err
}

func (f *Folder) PutObject(ctx context.Context, name string, content io.Reader) error {
	finalPath := f.filePath(name)
	if err := f.client.MkdirAll(ctx, path.Dir(finalPath), 0o755); err != nil {
		return err
	}
	tag, err := storage.NewTimestampRandomTag()
	if err != nil {
		return err
	}
	tmpPath := finalPath + tag
	file, err := f.client.OpenFile(ctx, tmpPath, syscall.O_CREAT|syscall.O_TRUNC|syscall.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cleanup := func() { _ = file.Close(); _ = f.client.Unlink(ctx, tmpPath) }
	reader := contextio.NewReader(ctx, content)
	buffer := make([]byte, 1024*1024)
	for {
		n, readErr := reader.Read(buffer)
		for written := 0; written < n; {
			wn, writeErr := file.WriteContext(ctx, buffer[written:n])
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
	if err = file.Close(); err != nil {
		_ = f.client.Unlink(ctx, tmpPath)
		return err
	}
	if err = f.client.Rename(ctx, tmpPath, finalPath); err != nil {
		_ = f.client.Unlink(ctx, tmpPath)
		return err
	}
	return nil
}

func (f *Folder) DeleteObjects(ctx context.Context, objects []storage.Object) error {
	for _, object := range objects {
		if err := f.client.Unlink(ctx, f.filePath(object.GetName())); err != nil && !errors.Is(err, syscall.ENOENT) {
			return err
		}
	}
	return nil
}

func (f *Folder) CopyObject(ctx context.Context, source, destination string) error {
	file, err := f.client.Open(ctx, path.Join(f.rootPath, source))
	if errors.Is(err, syscall.ENOENT) {
		return storage.NewObjectNotFoundError(source)
	}
	if err != nil {
		return err
	}
	defer file.Close()
	return f.PutObject(ctx, destination, file)
}

func (f *Folder) Validate(ctx context.Context) error {
	_, err := f.client.Stat(ctx, f.filePath(""))
	return err
}
func (f *Folder) SetVersioningEnabled(context.Context, bool) {}
func (f *Folder) GetVersioningEnabled(context.Context) bool  { return false }

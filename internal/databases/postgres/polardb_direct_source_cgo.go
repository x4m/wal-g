//go:build pfs && !pfsnative && linux && cgo

package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/pfsclient"
	"github.com/wal-g/wal-g/utility"
)

type cgoPolarDBDirectSource struct {
	client *pfsclient.Client
	root   string
}

func openPolarDBDirectSource(_ context.Context, root string) (polarDBDirectSource, error) {
	client, root, err := openCGOPFSClient(root, true)
	if err != nil {
		return nil, fmt.Errorf("mount PolarDB shared data source through C SDK: %w", err)
	}
	return &cgoPolarDBDirectSource{client: client, root: root}, nil
}

func openCGOPFSClient(root string, readOnly bool) (*pfsclient.Client, string, error) {
	root = "/" + strings.TrimLeft(root, "/")
	parts := strings.Split(strings.TrimPrefix(root, "/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		return nil, "", fmt.Errorf("%s must be /<device>/<polar-data-path>, got %q", PolarDBDirectDataPathEnv, root)
	}
	hostID := pfsclient.DefaultHostID
	if value, ok := conf.GetSetting(conf.PFSHostIDSetting); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return nil, "", fmt.Errorf("parse %s: expected a non-negative integer, got %q", conf.PFSHostIDSetting, value)
		}
		hostID = parsed
	}
	timeout, err := conf.GetDurationSettingDefault(conf.PFSDTimeoutSetting, pfsclient.DefaultTimeout)
	if err != nil {
		return nil, "", err
	}
	if timeout <= 0 {
		return nil, "", fmt.Errorf("%s must be positive, got %s", conf.PFSDTimeoutSetting, timeout)
	}
	cluster, _ := conf.GetSetting(conf.PFSClusterSetting)
	server, _ := conf.GetSetting(conf.PFSDServerAddressSetting)
	client, err := pfsclient.Open(pfsclient.Config{
		Device: parts[0], Cluster: cluster, HostID: hostID,
		Server: server, Timeout: timeout, ReadOnly: readOnly,
	})
	if err != nil {
		return nil, "", err
	}
	return client, root, nil
}

func (source *cgoPolarDBDirectSource) Close() error { return source.client.Close() }

func (source *cgoPolarDBDirectSource) Open(_ context.Context, filePath string) (io.ReadCloser, error) {
	cleanPath := "/" + strings.TrimLeft(filePath, "/")
	if cleanPath != source.root && !strings.HasPrefix(cleanPath, strings.TrimRight(source.root, "/")+"/") {
		return nil, fmt.Errorf("PolarDB shared file %q is outside configured root %q", cleanPath, source.root)
	}
	return source.client.OpenFile(cleanPath, os.O_RDONLY, 0)
}

func (source *cgoPolarDBDirectSource) AddToBundle(ctx context.Context, bundle *Bundle, pgData string) error {
	stats := polarDBDirectWalkStats{}
	if err := source.walk(ctx, bundle, pgData, source.root, "", &stats); err != nil {
		return err
	}
	if !stats.hasPgControl {
		return fmt.Errorf("PolarDB shared root %q does not contain global/pg_control", source.root)
	}
	if stats.nonEmptyFiles == 0 {
		return fmt.Errorf("PolarDB shared root %q contains no non-empty files", source.root)
	}
	tracelog.InfoLogger.Printf(
		"Discovered %d files (%d non-empty, %d bytes) under PolarDB shared root %s through C SDK",
		stats.files, stats.nonEmptyFiles, stats.bytes, source.root)
	return nil
}

func (source *cgoPolarDBDirectSource) walk(
	ctx context.Context, bundle *Bundle, pgData, current, relative string, stats *polarDBDirectWalkStats,
) error {
	entries, err := source.client.ReadDir(current)
	if err != nil {
		return fmt.Errorf("read PolarDB shared directory %q: %w", current, err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		remotePath := path.Join(current, entry.Name)
		relativePath := path.Join(relative, entry.Name)
		archivePath := filepath.Join(pgData, "polar_shared_data", filepath.FromSlash(relativePath))
		info := cgoPolarDBFileInfo{name: entry.Name, info: entry.FileInfo}
		if entry.IsDir {
			err = bundle.AddDirectFile(archivePath, info, nil)
			if err == filepath.SkipDir {
				continue
			}
			if err != nil {
				return err
			}
			if err = source.walk(ctx, bundle, pgData, remotePath, relativePath, stats); err != nil {
				return err
			}
			continue
		}
		stats.files++
		stats.bytes += entry.Size
		if entry.Size > 0 {
			stats.nonEmptyFiles++
		}
		if relativePath == "global/pg_control" {
			stats.hasPgControl = true
		}
		filePath := remotePath
		opener := func(openCtx context.Context) (io.ReadCloser, error) {
			return source.Open(openCtx, filePath)
		}
		if err = bundle.AddDirectFile(archivePath, info, opener); err != nil {
			return err
		}
	}
	return nil
}

type cgoPolarDBFileInfo struct {
	name string
	info pfsclient.FileInfo
}

func (info cgoPolarDBFileInfo) Name() string       { return info.name }
func (info cgoPolarDBFileInfo) Size() int64        { return info.info.Size }
func (info cgoPolarDBFileInfo) ModTime() time.Time { return info.info.ModTime }
func (info cgoPolarDBFileInfo) IsDir() bool        { return info.info.IsDir }
func (info cgoPolarDBFileInfo) Sys() any           { return nil }
func (info cgoPolarDBFileInfo) Mode() fs.FileMode {
	if info.info.IsDir {
		return fs.ModeDir | 0o700
	}
	return 0o600
}

func handlePolarDBWALFetch(
	ctx context.Context, baseReader internal.StorageFolderReader, walFileName, location string,
) (bool, error) {
	root := polarDBDirectDataPath()
	if root == "" {
		return false, nil
	}
	reader, err := internal.DownloadAndDecompressStorageFile(
		ctx, baseReader.SubFolder(utility.WalPath), walFileName)
	if err != nil {
		return true, err
	}
	defer utility.LoggedClose(reader, "close downloaded PolarDB WAL")

	client, root, err := openCGOPFSClient(root, false)
	if err != nil {
		return true, fmt.Errorf("mount PolarDB WAL destination through C SDK: %w", err)
	}
	defer utility.LoggedClose(client, "unmount PolarDB WAL destination")

	destination := polarDBWALDestination(root, location)
	if err = client.MkdirAll(path.Dir(destination)); err != nil {
		return true, fmt.Errorf("create PolarDB WAL directory: %w", err)
	}
	temporary := fmt.Sprintf("%s.wal-g.%d", destination, os.Getpid())
	file, err := client.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return true, fmt.Errorf("create temporary PolarDB WAL: %w", err)
	}
	_, copyErr := io.CopyBuffer(file, reader, make([]byte, pfsclient.MaxIOSize))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		_ = client.Remove(temporary)
		if copyErr != nil {
			return true, fmt.Errorf("write PolarDB WAL: %w", copyErr)
		}
		return true, fmt.Errorf("close PolarDB WAL: %w", closeErr)
	}
	if err = client.Rename(temporary, destination); err != nil {
		_ = client.Remove(temporary)
		return true, fmt.Errorf("publish PolarDB WAL: %w", err)
	}
	tracelog.InfoLogger.Printf("Fetched WAL %s directly to PolarDB shared storage at %s", walFileName, destination)
	return true, nil
}

func restorePolarDBSharedDataPlatform(ctx context.Context, pgData, root string) error {
	localRoot := filepath.Join(pgData, "polar_shared_data")
	info, err := os.Stat(localRoot)
	if err != nil {
		return fmt.Errorf("stat extracted PolarDB shared data: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("extracted PolarDB shared data %q is not a directory", localRoot)
	}

	client, root, err := openCGOPFSClient(root, false)
	if err != nil {
		return fmt.Errorf("mount PolarDB restore destination through C SDK: %w", err)
	}
	defer utility.LoggedClose(client, "unmount PolarDB restore destination")

	if _, err = client.Stat(root); err == nil {
		entries, readErr := client.ReadDir(root)
		if readErr != nil {
			return fmt.Errorf("inspect PolarDB restore destination: %w", readErr)
		}
		if len(entries) != 0 {
			return fmt.Errorf("PolarDB restore destination %q is not empty", root)
		}
	} else if errors.Is(err, syscall.ENOENT) {
		if err = client.MkdirAll(root); err != nil {
			return fmt.Errorf("create PolarDB restore destination: %w", err)
		}
	} else {
		return fmt.Errorf("stat PolarDB restore destination: %w", err)
	}

	var files []string
	err = filepath.Walk(localRoot, func(name string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(localRoot, name)
		if err != nil {
			return err
		}
		remote := path.Join(root, filepath.ToSlash(relative))
		if info.IsDir() {
			return client.MkdirAll(remote)
		}
		files = append(files, name)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk extracted PolarDB shared data: %w", err)
	}
	sort.SliceStable(files, func(i, j int) bool {
		return filepath.ToSlash(strings.TrimPrefix(files[i], localRoot+string(filepath.Separator))) != "global/pg_control" &&
			filepath.ToSlash(strings.TrimPrefix(files[j], localRoot+string(filepath.Separator))) == "global/pg_control"
	})
	for _, name := range files {
		if err = copyLocalFileToPFS(ctx, client, localRoot, root, name); err != nil {
			return err
		}
	}
	if err = os.RemoveAll(localRoot); err != nil {
		return fmt.Errorf("remove restored PolarDB shared staging directory: %w", err)
	}
	tracelog.InfoLogger.Printf("Restored %d PolarDB shared files directly through PFSD", len(files))
	return nil
}

func copyLocalFileToPFS(
	ctx context.Context, client *pfsclient.Client, localRoot, remoteRoot, name string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, err := os.Open(name)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(source, "close restored PolarDB source file")
	relative, err := filepath.Rel(localRoot, name)
	if err != nil {
		return err
	}
	destination := path.Join(remoteRoot, filepath.ToSlash(relative))
	target, err := client.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create restored PolarDB file %q: %w", destination, err)
	}
	_, copyErr := io.CopyBuffer(target, source, make([]byte, pfsclient.MaxIOSize))
	closeErr := target.Close()
	if copyErr != nil {
		return fmt.Errorf("write restored PolarDB file %q: %w", destination, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close restored PolarDB file %q: %w", destination, closeErr)
	}
	return nil
}

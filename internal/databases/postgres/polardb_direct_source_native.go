//go:build pfsnative && !pfs && linux && amd64

package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/pfsnative"
	"github.com/wal-g/wal-g/utility"
)

type nativePolarDBDirectSource struct {
	client *pfsnative.Client
	root   string
}

func openPolarDBDirectSource(ctx context.Context, root string) (polarDBDirectSource, error) {
	client, root, err := openNativePFSClient(ctx, root, true)
	if err != nil {
		return nil, fmt.Errorf("mount PolarDB shared data source through native Go client: %w", err)
	}
	return &nativePolarDBDirectSource{client: client, root: root}, nil
}

func openNativePFSClient(ctx context.Context, root string, readOnly bool) (*pfsnative.Client, string, error) {
	root = "/" + strings.TrimLeft(root, "/")
	parts := strings.Split(strings.TrimPrefix(root, "/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		return nil, "", fmt.Errorf("%s must be /<device>/<polar-data-path>, got %q", PolarDBDirectDataPathEnv, root)
	}
	hostID := 1
	if value := os.Getenv("WALG_PFS_HOST_ID"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return nil, "", fmt.Errorf("parse WALG_PFS_HOST_ID: expected a non-negative integer, got %q", value)
		}
		hostID = parsed
	}
	timeout := pfsnative.DefaultTimeout
	if value := os.Getenv("WALG_PFSD_TIMEOUT"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return nil, "", fmt.Errorf("parse WALG_PFSD_TIMEOUT: expected a positive duration, got %q", value)
		}
		timeout = parsed
	}
	flags := pfsnative.ReadWrite
	if readOnly {
		flags = pfsnative.ReadOnly
	}
	mountCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, err := pfsnative.Mount(mountCtx, pfsnative.Config{
		ServerDir: polarDBPFSDServerDir(parts[0]),
		Cluster:   os.Getenv("WALG_PFS_CLUSTER"),
		PBD:       parts[0],
		HostID:    hostID,
		Flags:     flags,
		Timeout:   timeout,
	})
	if err != nil {
		return nil, "", err
	}
	return client, root, nil
}

func polarDBPFSDServerDir(pbd string) string {
	server := os.Getenv("WALG_PFSD_SERVER_ADDR")
	if server == "" {
		server = pfsnative.DefaultServerDir
	}
	return path.Join(server, pbd)
}

func handlePolarDBWALFetch(
	ctx context.Context, baseReader internal.StorageFolderReader, walFileName string,
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

	client, root, err := openNativePFSClient(ctx, root, false)
	if err != nil {
		return true, fmt.Errorf("mount PolarDB WAL destination through native Go client: %w", err)
	}
	defer utility.LoggedClose(client, "unmount PolarDB WAL destination")

	destination := polarDBWALDestination(root, walFileName)
	if err = client.MkdirAll(ctx, path.Dir(destination), 0o700); err != nil {
		return true, fmt.Errorf("create PolarDB WAL directory: %w", err)
	}
	temporary := fmt.Sprintf("%s.wal-g.%d", destination, os.Getpid())
	file, err := client.OpenFile(ctx, temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return true, fmt.Errorf("create temporary PolarDB WAL: %w", err)
	}
	_, copyErr := io.CopyBuffer(file, reader, make([]byte, pfsnative.MaxIOSize))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		_ = client.Remove(ctx, temporary)
		if copyErr != nil {
			return true, fmt.Errorf("write PolarDB WAL: %w", copyErr)
		}
		return true, fmt.Errorf("close PolarDB WAL: %w", closeErr)
	}
	if err = client.Rename(ctx, temporary, destination); err != nil {
		_ = client.Remove(ctx, temporary)
		return true, fmt.Errorf("publish PolarDB WAL: %w", err)
	}
	tracelog.InfoLogger.Printf("Fetched WAL %s directly to PolarDB shared storage", walFileName)
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

	client, root, err := openNativePFSClient(ctx, root, false)
	if err != nil {
		return fmt.Errorf("mount PolarDB restore destination through native Go client: %w", err)
	}
	defer utility.LoggedClose(client, "unmount PolarDB restore destination")

	if _, err = client.Stat(ctx, root); err == nil {
		entries, readErr := client.ReadDir(ctx, root)
		if readErr != nil {
			return fmt.Errorf("inspect PolarDB restore destination: %w", readErr)
		}
		if len(entries) != 0 {
			return fmt.Errorf("PolarDB restore destination %q is not empty", root)
		}
	} else if errors.Is(err, syscall.ENOENT) {
		if err = client.MkdirAll(ctx, root, 0o700); err != nil {
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
			return client.MkdirAll(ctx, remote, 0o700)
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
		if err = copyLocalFileToNativePFS(ctx, client, localRoot, root, name); err != nil {
			return err
		}
	}
	if err = os.RemoveAll(localRoot); err != nil {
		return fmt.Errorf("remove restored PolarDB shared staging directory: %w", err)
	}
	tracelog.InfoLogger.Printf("Restored %d PolarDB shared files directly through PFSD", len(files))
	return nil
}

func copyLocalFileToNativePFS(
	ctx context.Context, client *pfsnative.Client, localRoot, remoteRoot, name string,
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
	target, err := client.OpenFile(ctx, destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create restored PolarDB file %q: %w", destination, err)
	}
	_, copyErr := io.CopyBuffer(target, source, make([]byte, pfsnative.MaxIOSize))
	closeErr := target.Close()
	if copyErr != nil {
		return fmt.Errorf("write restored PolarDB file %q: %w", destination, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close restored PolarDB file %q: %w", destination, closeErr)
	}
	return nil
}

func (source *nativePolarDBDirectSource) Close() error { return source.client.Close() }

func (source *nativePolarDBDirectSource) Open(ctx context.Context, filePath string) (io.ReadCloser, error) {
	cleanPath := "/" + strings.TrimLeft(filePath, "/")
	if cleanPath != source.root && !strings.HasPrefix(cleanPath, strings.TrimRight(source.root, "/")+"/") {
		return nil, fmt.Errorf("PolarDB shared file %q is outside configured root %q", cleanPath, source.root)
	}
	return source.client.Open(ctx, cleanPath)
}

func (source *nativePolarDBDirectSource) AddToBundle(ctx context.Context, bundle *Bundle, pgData string) error {
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
		"Discovered %d files (%d non-empty, %d bytes) under PolarDB shared root %s",
		stats.files, stats.nonEmptyFiles, stats.bytes, source.root)
	return nil
}

func (source *nativePolarDBDirectSource) walk(
	ctx context.Context, bundle *Bundle, pgData, current, relative string, stats *polarDBDirectWalkStats,
) error {
	entries, err := source.client.ReadDir(ctx, current)
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
		if entry.IsDir() {
			err = bundle.AddDirectFile(archivePath, entry.FileInfo, nil)
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
		stats.bytes += entry.Size()
		if entry.Size() > 0 {
			stats.nonEmptyFiles++
		}
		if relativePath == "global/pg_control" {
			stats.hasPgControl = true
		}
		filePath := remotePath
		opener := func(openCtx context.Context) (io.ReadCloser, error) {
			return source.Open(openCtx, filePath)
		}
		if err = bundle.AddDirectFile(archivePath, entry.FileInfo, opener); err != nil {
			return err
		}
	}
	return nil
}

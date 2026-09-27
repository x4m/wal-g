//go:build pfsnative && !pfs && linux && amd64

package postgres

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
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
	if value, ok := conf.GetSetting(conf.PFSHostIDSetting); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return nil, "", fmt.Errorf("parse %s: expected a non-negative integer, got %q", conf.PFSHostIDSetting, value)
		}
		hostID = parsed
	}
	timeout, err := conf.GetDurationSettingDefault(conf.PFSDTimeoutSetting, pfsnative.DefaultTimeout)
	if err != nil {
		return nil, "", err
	}
	if timeout <= 0 {
		return nil, "", fmt.Errorf("%s must be positive, got %s", conf.PFSDTimeoutSetting, timeout)
	}
	flags := pfsnative.ReadWrite
	if readOnly {
		flags = pfsnative.ReadOnly
	}
	mountCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cluster, _ := conf.GetSetting(conf.PFSClusterSetting)
	client, err := pfsnative.Mount(mountCtx, pfsnative.Config{
		ServerDir: polarDBPFSDServerDir(parts[0]),
		Cluster:   cluster,
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
	server, _ := conf.GetSetting(conf.PFSDServerAddressSetting)
	if server == "" {
		server = pfsnative.DefaultServerDir
	}
	if strings.HasSuffix(server, ".socket") {
		return server
	}
	return path.Join(server, pbd)
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

	client, root, err := openNativePFSClient(ctx, root, false)
	if err != nil {
		return true, fmt.Errorf("mount PolarDB WAL destination through native Go client: %w", err)
	}
	defer utility.LoggedClose(client, "unmount PolarDB WAL destination")

	destination := polarDBWALDestination(root, location)
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
	tracelog.InfoLogger.Printf("Fetched WAL %s directly to PolarDB shared storage at %s", walFileName, destination)
	return true, nil
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

//go:build pfsnative && linux && amd64

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
	"github.com/wal-g/wal-g/pkg/pfsnative"
)

type nativePolarDBDirectSource struct {
	client *pfsnative.Client
	root   string
}

func openPolarDBDirectSource(ctx context.Context, root string) (polarDBDirectSource, error) {
	root = "/" + strings.TrimLeft(root, "/")
	parts := strings.Split(strings.TrimPrefix(root, "/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		return nil, fmt.Errorf("%s must be /<device>/<polar-data-path>, got %q", PolarDBDirectDataPathEnv, root)
	}
	hostID := 1
	if value := os.Getenv("WALG_PFS_HOST_ID"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("parse WALG_PFS_HOST_ID: expected a non-negative integer, got %q", value)
		}
		hostID = parsed
	}
	client, err := pfsnative.Mount(ctx, pfsnative.Config{
		ServerDir: polarDBPFSDServerDir(parts[0]),
		Cluster:   os.Getenv("WALG_PFS_CLUSTER"),
		PBD:       parts[0],
		HostID:    hostID,
		Flags:     pfsnative.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("mount PolarDB shared data source: %w", err)
	}
	return &nativePolarDBDirectSource{client: client, root: root}, nil
}

func polarDBPFSDServerDir(pbd string) string {
	server := os.Getenv("WALG_PFSD_SERVER_ADDR")
	if server == "" {
		server = pfsnative.DefaultServerDir
	}
	return path.Join(server, pbd)
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

type polarDBDirectWalkStats struct {
	files         int64
	nonEmptyFiles int64
	bytes         int64
	hasPgControl  bool
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

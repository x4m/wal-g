//go:build pfsnative && linux && amd64

package pfsnative

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	client "github.com/wal-g/wal-g/pkg/pfsnative"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = (*Storage)(nil)

type config struct {
	RootPath string
	PBDName  string
	Cluster  string
	HostID   int
	Server   string
	Timeout  time.Duration
}

type Storage struct {
	rootFolder storage.Folder
	hash       string
	client     *client.Client
}

func newStorage(ctx context.Context, cfg config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	server := cfg.Server
	if server == "" {
		server = client.DefaultServerDir
	}
	mountCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	var pfsClient *client.Client
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		serverAddress := server
		if !strings.HasSuffix(serverAddress, ".socket") {
			serverAddress = path.Join(serverAddress, cfg.PBDName)
		}
		pfsClient, err = client.Mount(mountCtx, client.Config{
			ServerDir: serverAddress, Cluster: cfg.Cluster,
			PBD: cfg.PBDName, HostID: cfg.HostID, Flags: client.ReadWrite,
		})
		if err == nil {
			break
		}
		var daemonErr *client.DaemonError
		if !errors.As(err, &daemonErr) || attempt == 2 {
			return nil, err
		}
		delay := time.Duration(25*(1<<attempt)) * time.Millisecond
		select {
		case <-mountCtx.Done():
			return nil, errors.Join(err, mountCtx.Err())
		case <-time.After(delay):
		}
	}
	if err != nil {
		return nil, err
	}
	info, err := pfsClient.Stat(mountCtx, cfg.RootPath)
	if err != nil || !info.IsDir() {
		_ = pfsClient.Close()
		if err != nil {
			return nil, fmt.Errorf("validate PFS root %q: %w", cfg.RootPath, err)
		}
		return nil, fmt.Errorf("PFS root %q is not a directory", cfg.RootPath)
	}
	var root storage.Folder = &Folder{client: pfsClient, rootPath: path.Clean(cfg.RootPath)}
	for _, wrap := range rootWraps {
		root = wrap(root)
	}
	hash, err := storage.ComputeConfigHash("pfs-native", cfg)
	if err != nil {
		_ = pfsClient.Close()
		return nil, fmt.Errorf("compute config hash: %w", err)
	}
	return &Storage{rootFolder: root, hash: hash, client: pfsClient}, nil
}

func (s *Storage) RootFolder() storage.Folder { return s.rootFolder }
func (s *Storage) ConfigHash() string         { return s.hash }
func (s *Storage) Close() error               { return s.client.Close() }

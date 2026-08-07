//go:build pfs && linux && cgo

package pfs

import (
	"fmt"
	"path"
	"time"

	"github.com/wal-g/wal-g/pkg/pfsclient"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = (*Storage)(nil)

type config struct {
	RootPath  string
	PBDName   string
	Cluster   string
	HostID    int
	Server    string
	TimeoutMS int
}

type Storage struct {
	rootFolder storage.Folder
	hash       string
	client     *pfsclient.Client
}

func newStorage(cfg config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	client, err := pfsclient.Open(pfsclient.Config{
		Device: cfg.PBDName, Cluster: cfg.Cluster, HostID: cfg.HostID,
		Server: cfg.Server, Timeout: time.Duration(cfg.TimeoutMS) * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	if info, err := client.Stat(cfg.RootPath); err != nil || !info.IsDir {
		_ = client.Close()
		if err != nil {
			return nil, fmt.Errorf("validate PFS root %q: %w", cfg.RootPath, err)
		}
		return nil, fmt.Errorf("PFS root %q is not a directory", cfg.RootPath)
	}

	var root storage.Folder = &Folder{client: client, rootPath: path.Clean(cfg.RootPath)}
	for _, wrap := range rootWraps {
		root = wrap(root)
	}
	hash, err := storage.ComputeConfigHash("pfs", cfg)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("compute config hash: %w", err)
	}
	return &Storage{rootFolder: root, hash: hash, client: client}, nil
}

func (s *Storage) RootFolder() storage.Folder { return s.rootFolder }
func (s *Storage) ConfigHash() string         { return s.hash }
func (s *Storage) Close() error               { return s.client.Close() }

//go:build pfs && linux && cgo

// Package pfsclient provides direct access to PolarDB File System through
// the pfsdaemon client SDK. It does not require a FUSE mount.
package pfsclient

import (
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"syscall"
	"time"
)

const (
	DefaultCluster = "polarstore"
	DefaultHostID  = 1
	DefaultTimeout = 5 * time.Second
)

// Config describes one pfsdaemon connection. Device is the PFS block-device
// name, without leading or trailing slashes.
type Config struct {
	Device  string
	Cluster string
	HostID  int
	Server  string
	Timeout time.Duration
}

// FileInfo is the subset of PFS metadata commonly needed by applications.
type FileInfo struct {
	Size    int64
	ModTime time.Time
	IsDir   bool
}

// DirEntry describes a direct child returned by ReadDir.
type DirEntry struct {
	Name string
	FileInfo
}

// The upstream SDK maintains process-global mount state. Serialize lifecycle
// operations and share one compatible mount between clients in the process.
var mountState struct {
	sync.Mutex
	config Config
	refs   int
}

type Client struct {
	config Config
	once   sync.Once
	err    error
}

func Open(config Config) (*Client, error) {
	if config.Device == "" || path.Base(config.Device) != config.Device {
		return nil, fmt.Errorf("invalid PFS device %q", config.Device)
	}
	if config.Cluster == "" {
		config.Cluster = DefaultCluster
	}
	if config.HostID == 0 {
		config.HostID = DefaultHostID
	}
	if config.HostID < 0 {
		return nil, fmt.Errorf("PFS host ID must not be negative")
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultTimeout
	}
	if config.Timeout < 0 {
		return nil, fmt.Errorf("PFS timeout must be positive")
	}

	mountState.Lock()
	defer mountState.Unlock()
	if mountState.refs > 0 {
		if mountState.config != config {
			return nil, fmt.Errorf("pfsd SDK already mounted with a different configuration")
		}
		mountState.refs++
		return &Client{config: config}, nil
	}
	sdkSetServer(config.Server)
	if err := sdkMount(config.Cluster, config.Device, config.HostID, int(config.Timeout/time.Millisecond)); err != nil {
		return nil, err
	}
	mountState.config = config
	mountState.refs = 1
	return &Client{config: config}, nil
}

func (c *Client) Close() error {
	c.once.Do(func() {
		mountState.Lock()
		defer mountState.Unlock()
		if mountState.refs == 0 {
			return
		}
		mountState.refs--
		if mountState.refs == 0 {
			c.err = sdkUnmount(c.config.Device)
			mountState.config = Config{}
		}
	})
	return c.err
}

func (c *Client) Stat(name string) (FileInfo, error) {
	info, err := sdkStat(name)
	return FileInfo{Size: info.size, ModTime: info.mtime, IsDir: info.isDir}, err
}

func (c *Client) Mkdir(name string) error { return sdkMkdir(name) }
func (c *Client) Remove(name string) error {
	info, err := c.Stat(name)
	if err != nil {
		return err
	}
	if info.IsDir {
		return sdkRmdir(name)
	}
	return sdkUnlink(name)
}
func (c *Client) Rename(oldName, newName string) error { return sdkRename(oldName, newName) }

func (c *Client) OpenFile(name string, flags int, mode uint32) (*File, error) {
	fd, err := sdkOpen(name, flags, mode)
	if err != nil {
		return nil, err
	}
	return &File{fd: fd, name: name}, nil
}

func (c *Client) ReadDir(name string) ([]DirEntry, error) {
	names, err := sdkReadDir(name)
	if err != nil {
		return nil, err
	}
	entries := make([]DirEntry, 0, len(names))
	for _, child := range names {
		info, err := c.Stat(path.Join(name, child))
		if err != nil {
			return nil, err
		}
		entries = append(entries, DirEntry{Name: child, FileInfo: info})
	}
	return entries, nil
}

func (c *Client) MkdirAll(name string) error {
	info, err := c.Stat(name)
	if err == nil {
		if info.IsDir {
			return nil
		}
		return fmt.Errorf("PFS path %q is not a directory", name)
	}
	if !errors.Is(err, syscall.ENOENT) {
		return err
	}
	parent := path.Dir(name)
	if parent != name {
		if err := c.MkdirAll(parent); err != nil {
			return err
		}
	}
	if err := c.Mkdir(name); err != nil && !errors.Is(err, syscall.EEXIST) {
		return err
	}
	return nil
}

type File struct {
	fd   int
	name string
	once sync.Once
	err  error
}

func (f *File) Name() string { return f.name }
func (f *File) Read(buffer []byte) (int, error) {
	n, err := sdkRead(f.fd, buffer)
	if err == nil && n == 0 {
		err = io.EOF
	}
	return n, err
}
func (f *File) Write(buffer []byte) (int, error) { return sdkWrite(f.fd, buffer) }
func (f *File) Close() error {
	f.once.Do(func() { f.err = sdkClose(f.fd) })
	return f.err
}

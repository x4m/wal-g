//go:build linux && amd64

package pfsnative

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	DefaultServerDir = "/var/run/pfsd"
	DefaultCluster   = "polarstore"
	DefaultTimeout   = 5 * time.Second
	MaxIOSize        = 1 << 20

	ReadOnly  = 0x01 | 0x10
	ReadWrite = 0x01 | 0x02 | 0x10
)

type Config struct {
	ServerDir string
	Cluster   string
	PBD       string
	HostID    int
	Flags     int
	Timeout   time.Duration
}

// Client is a native-Go PFSD transport connection.
type Client struct {
	mu       sync.Mutex
	rpcMu    sync.Mutex
	opMu     sync.RWMutex
	pidPath  string
	pidFile  *os.File
	metaLock *os.File
	hostLock *os.File
	pbdRoot  string
	ack      mountAck
	mappings [][]byte
	closed   bool
	timeout  time.Duration
	socket   *socketTransport
}

func Mount(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.ServerDir == "" {
		cfg.ServerDir = DefaultServerDir
	}
	if cfg.Cluster == "" {
		cfg.Cluster = DefaultCluster
	}
	if cfg.Flags == 0 {
		cfg.Flags = ReadWrite
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.HostID < 0 || cfg.HostID > int(^uint32(0)>>1) {
		return nil, fmt.Errorf("PFSD host ID is out of range: %d", cfg.HostID)
	}
	if _, err := encodeMountRequest(mountRequest{Cluster: cfg.Cluster, PBD: cfg.PBD}); err != nil {
		return nil, err
	}

	// Recent PFSD versions expose a Unix-socket/memfd transport. Prefer it
	// when its well-known socket exists, while retaining compatibility with
	// the older pidfile/shared-memory protocol below.
	if socketPath := socketPathFor(cfg); socketPath != "" {
		if _, err := os.Stat(socketPath); err == nil {
			return mountSocket(ctx, cfg, socketPath)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect PFSD socket %q: %w", socketPath, err)
		}
	}

	c := &Client{
		pidPath: filepath.Join(cfg.ServerDir, fmt.Sprintf("%d.pid", os.Getpid())),
		pbdRoot: "/" + cfg.PBD,
		timeout: cfg.Timeout,
	}
	if cfg.Flags&ReadWrite == ReadWrite {
		if err := c.acquireMountLocks(ctx, cfg.PBD, cfg.HostID); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(c.pidPath, os.O_RDWR|os.O_CREATE|os.O_EXCL|unix.O_SYNC, 0o644)
	if err != nil {
		c.closeMountLocks()
		return nil, fmt.Errorf("create PFSD pidfile %q: %w", c.pidPath, err)
	}
	c.pidFile = f
	ok := false
	defer func() {
		if !ok {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = c.close(cleanupCtx, true, true)
		}
	}()

	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock PFSD pidfile %q: %w", c.pidPath, err)
	}

	oldAckBytes := make([]byte, pageSize)
	oldEpoch := uint32(0)
	if n, readErr := f.ReadAt(oldAckBytes, pageSize); readErr == nil && n == pageSize {
		oldAck, decodeErr := decodeMountAck(oldAckBytes)
		if decodeErr != nil {
			return nil, decodeErr
		}
		oldEpoch = oldAck.Epoch
	} else if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, fmt.Errorf("read old PFSD mount acknowledgement: %w", readErr)
	}

	req, err := encodeMountRequest(mountRequest{
		Cluster: cfg.Cluster, PBD: cfg.PBD, HostID: int32(cfg.HostID),
		Flags: int32(cfg.Flags), Epoch: oldEpoch + 1,
	})
	if err != nil {
		return nil, err
	}
	if n, writeErr := f.WriteAt(req, 0); writeErr != nil || n != len(req) {
		return nil, writeFailure("write PFSD mount request", n, len(req), writeErr)
	}
	if err = f.Chmod(0o666); err != nil {
		return nil, fmt.Errorf("signal PFSD mount request: %w", err)
	}

	ack, err := waitMountAck(ctx, f, oldEpoch+1)
	if err != nil {
		return nil, err
	}
	if ack.Version != protocolVersion {
		return nil, fmt.Errorf("unsupported PFSD protocol version: got %d, want %d", ack.Version, protocolVersion)
	}
	if ack.Error != 0 {
		return nil, &DaemonError{Code: ack.Error, Message: ack.Message}
	}
	if ack.ConnectID < 0 || ack.MountID < 0 {
		return nil, fmt.Errorf("invalid PFSD mount acknowledgement: connection=%d mount=%d", ack.ConnectID, ack.MountID)
	}

	c.ack = ack
	if c.metaLock != nil {
		_ = c.metaLock.Close()
		c.metaLock = nil
	}
	for i, name := range ack.ShmNames {
		if name == "" {
			return nil, fmt.Errorf("PFSD shared memory filename %d is empty", i)
		}
		mapping, mapErr := mapShm(name)
		if mapErr != nil {
			return nil, mapErr
		}
		c.mappings = append(c.mappings, mapping)
	}
	ok = true
	return c, nil
}

type DaemonError struct {
	Code    int32
	Message string
}

func (e *DaemonError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("PFSD rejected mount with error %d", e.Code)
	}
	return fmt.Sprintf("PFSD rejected mount with error %d: %s", e.Code, e.Message)
}

func (e *DaemonError) Unwrap() error {
	if e.Code >= 0 {
		return nil
	}
	return syscall.Errno(-e.Code)
}

func (c *Client) ConnectionID() int32 {
	if c.socket != nil {
		return int32(c.socket.connectionID)
	}
	return c.ack.ConnectID
}
func (c *Client) MountID() int32 { return c.ack.MountID }

func (c *Client) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.CloseContext(ctx)
}

func (c *Client) CloseContext(ctx context.Context) error {
	c.rpcMu.Lock()
	defer c.rpcMu.Unlock()
	c.opMu.Lock()
	defer c.opMu.Unlock()
	return c.close(ctx, true, true)
}

func (c *Client) close(ctx context.Context, signalUnmount, wait bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	defer c.closeMountLocks()
	if c.socket != nil {
		err := c.socket.close()
		c.socket = nil
		return err
	}

	var errs []error
	for _, mapping := range c.mappings {
		if err := unix.Munmap(mapping); err != nil {
			errs = append(errs, fmt.Errorf("unmap PFSD shared memory: %w", err))
		}
	}
	c.mappings = nil
	if c.pidFile == nil {
		return errors.Join(errs...)
	}
	if err := unix.Flock(int(c.pidFile.Fd()), unix.LOCK_UN); err != nil {
		errs = append(errs, fmt.Errorf("unlock PFSD pidfile: %w", err))
	}
	if signalUnmount {
		if err := c.pidFile.Chmod(0o777); err != nil {
			errs = append(errs, fmt.Errorf("signal PFSD unmount: %w", err))
		}
	}
	if err := c.pidFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close PFSD pidfile: %w", err))
	}
	c.pidFile = nil
	if wait {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			_, err := os.Stat(c.pidPath)
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("wait for PFSD unmount: %w", err))
				break
			}
			select {
			case <-ctx.Done():
				errs = append(errs, fmt.Errorf("wait for PFSD unmount: %w", ctx.Err()))
				return errors.Join(errs...)
			case <-ticker.C:
			}
		}
	}
	return errors.Join(errs...)
}

func (c *Client) acquireMountLocks(ctx context.Context, pbd string, hostID int) error {
	lockPath := filepath.Join("/var/run/pfs", pbd+"-paxos-hostid")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return fmt.Errorf("open PFSD host-ID lock %q: %w", lockPath, err)
	}
	c.metaLock = f
	for {
		err = unix.FcntlFlock(f.Fd(), unix.F_SETLK, &unix.Flock_t{
			Type: unix.F_WRLCK, Whence: io.SeekStart, Start: 255 * 1024, Len: 1024,
		})
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EACCES) && !errors.Is(err, syscall.EAGAIN) {
			c.closeMountLocks()
			return fmt.Errorf("lock PFSD mount metadata: %w", err)
		}
		select {
		case <-ctx.Done():
			c.closeMountLocks()
			return fmt.Errorf("lock PFSD mount metadata: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	host, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		c.closeMountLocks()
		return fmt.Errorf("open PFSD host-ID lock %q: %w", lockPath, err)
	}
	c.hostLock = host
	length := int64(1024)
	if hostID == 0 {
		length = 0
	}
	err = unix.FcntlFlock(host.Fd(), unix.F_SETLK, &unix.Flock_t{
		Type: unix.F_WRLCK, Whence: io.SeekStart, Start: int64(hostID) * 1024, Len: length,
	})
	if err != nil {
		c.closeMountLocks()
		return fmt.Errorf("lock PFSD host ID %d: %w", hostID, err)
	}
	return nil
}

func (c *Client) closeMountLocks() {
	if c.metaLock != nil {
		_ = c.metaLock.Close()
		c.metaLock = nil
	}
	if c.hostLock != nil {
		_ = c.hostLock.Close()
		c.hostLock = nil
	}
}

func waitMountAck(ctx context.Context, f *os.File, epoch uint32) (mountAck, error) {
	ticker := time.NewTicker(100 * time.Microsecond)
	defer ticker.Stop()
	for {
		st, err := f.Stat()
		if err != nil {
			return mountAck{}, fmt.Errorf("stat PFSD pidfile: %w", err)
		}
		if st.Size() >= pidFileSize {
			b := make([]byte, pageSize)
			n, readErr := f.ReadAt(b, pageSize)
			if readErr != nil || n != len(b) {
				return mountAck{}, writeFailure("read PFSD mount acknowledgement", n, len(b), readErr)
			}
			ack, decodeErr := decodeMountAck(b)
			if decodeErr != nil {
				return mountAck{}, decodeErr
			}
			if ack.Epoch >= epoch {
				return ack, nil
			}
		}
		select {
		case <-ctx.Done():
			return mountAck{}, fmt.Errorf("wait for PFSD mount acknowledgement: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func mapShm(name string) ([]byte, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open PFSD shared memory %q: %w", name, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat PFSD shared memory %q: %w", name, err)
	}
	if st.Size() < pageSize || st.Size() > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("invalid PFSD shared memory %q size: %d", name, st.Size())
	}
	b, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("map PFSD shared memory %q: %w", name, err)
	}
	h, err := decodeShmHeader(b)
	if err == nil && h.Magic != shmMagic {
		err = fmt.Errorf("invalid PFSD shared memory %q magic: %#x", name, h.Magic)
	}
	if err == nil && h.Version != shmVersion {
		err = fmt.Errorf("unsupported PFSD shared memory %q version: %d", name, h.Version)
	}
	// Some PFSD v2 builds leave the optional shared-memory index field zero in
	// every region. The daemon acknowledgement already supplies the region
	// names, and request routing uses the validated unit size from each header.
	if err != nil {
		_ = unix.Munmap(b)
		return nil, err
	}
	return b, nil
}

func writeFailure(op string, got, want int, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return fmt.Errorf("%s: %w (got %d bytes, want %d)", op, syscall.EIO, got, want)
}

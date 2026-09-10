//go:build linux && amd64

package pfsnative

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	socketClientHello     = 0
	socketServerHello     = 1
	socketRegisterQueues  = 2
	socketRegisterBuffers = 3
	socketAck             = 4

	socketRequestSize   = 768
	socketRequestUnion  = 128
	socketResponseUnion = 448
	socketResponseSize  = 280
	socketSemOffset     = 40
	socketIOSize        = MaxIOSize

	queueCapacityOffset  = 0
	queueHeadOffset      = 64
	queueSlotsOffset     = 192
	queueSlotSize        = 64
	queueSlotTurnOffset  = 0
	queueSlotEmptyOffset = 8
	queueSlotValueOffset = 24
	futexWakeBitset      = 10
)

type socketTransport struct {
	mu           sync.Mutex
	conn         *net.UnixConn
	readBuf      []byte
	queuedFDs    []int
	queues       [][]byte
	requestFD    int
	request      []byte
	ioFD         int
	io           []byte
	connectionID uint64
	closed       bool
}

func socketPathFor(cfg Config) string {
	if filepath.Ext(cfg.ServerDir) == ".socket" {
		return cfg.ServerDir
	}
	return filepath.Join("/var/run", "pfsd-"+cfg.PBD+".socket")
}

func mountSocket(ctx context.Context, cfg Config, socketPath string) (*Client, error) {
	c := &Client{pbdRoot: "/" + cfg.PBD, timeout: cfg.Timeout}
	if cfg.Flags&ReadWrite == ReadWrite {
		if err := c.acquireMountLocks(ctx, cfg.PBD, cfg.HostID); err != nil {
			return nil, err
		}
	}
	t, err := newSocketTransport(ctx, cfg, socketPath)
	if err != nil {
		c.closeMountLocks()
		return nil, err
	}
	c.socket = t
	return c, nil
}

func newSocketTransport(ctx context.Context, cfg Config, socketPath string) (_ *socketTransport, retErr error) {
	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("connect to PFSD socket %q: %w", socketPath, err)
	}
	conn := raw.(*net.UnixConn)
	t := &socketTransport{conn: conn, requestFD: -1, ioFD: -1}
	defer func() {
		if retErr != nil {
			_ = t.close()
		}
	}()

	for {
		typ, msg, fds, err := t.readMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("receive PFSD queues: %w", err)
		}
		if typ != socketRegisterQueues {
			return nil, fmt.Errorf("unexpected PFSD handshake message %d", typ)
		}
		descs, last, err := decodeBufferMessage(msg)
		if err != nil {
			return nil, err
		}
		if len(descs) != len(fds) {
			return nil, fmt.Errorf("PFSD sent %d queue descriptors and %d file descriptors", len(descs), len(fds))
		}
		for i, desc := range descs {
			mapping, err := mapFD(fds[i], desc.size)
			if err != nil {
				closeFDs(fds[i:])
				return nil, fmt.Errorf("map PFSD queue %d: %w", desc.id, err)
			}
			_ = unix.Close(fds[i])
			t.queues = append(t.queues, mapping)
		}
		if last {
			break
		}
	}
	if len(t.queues) == 0 {
		return nil, errors.New("PFSD supplied no request queues")
	}

	if err := t.writeMessage(ctx, encodeClientHello(cfg), nil); err != nil {
		return nil, err
	}
	typ, msg, fds, err := t.readMessage(ctx)
	closeFDs(fds)
	if err != nil {
		return nil, fmt.Errorf("receive PFSD server hello: %w", err)
	}
	if typ != socketServerHello || len(msg) < 24 {
		return nil, fmt.Errorf("invalid PFSD server hello")
	}
	t.connectionID = binary.LittleEndian.Uint64(msg[12:20])
	if code := int32(binary.LittleEndian.Uint32(msg[20:24])); code != 0 {
		return nil, &DaemonError{Code: code}
	}

	t.requestFD, t.request, err = createMemFD("wal-g-pfs-request", pageSize)
	if err != nil {
		return nil, err
	}
	t.ioFD, t.io, err = createMemFD("wal-g-pfs-io", socketIOSize)
	if err != nil {
		return nil, err
	}
	register := encodeRegisterBuffers([]bufferDesc{{id: 0, size: uint64(len(t.request))}, {id: 1, size: uint64(len(t.io))}})
	if err = t.writeMessage(ctx, register, []int{t.requestFD, t.ioFD}); err != nil {
		return nil, err
	}
	typ, _, fds, err = t.readMessage(ctx)
	closeFDs(fds)
	if err != nil {
		return nil, fmt.Errorf("receive PFSD buffer acknowledgement: %w", err)
	}
	if typ != socketAck {
		return nil, fmt.Errorf("unexpected PFSD buffer acknowledgement %d", typ)
	}
	return t, nil
}

type bufferDesc struct{ id, size uint64 }

func encodeClientHello(cfg Config) []byte {
	n := 36 + len(cfg.Cluster) + len(cfg.PBD)
	b := make([]byte, n)
	putU32 := func(off int, v uint32) { binary.LittleEndian.PutUint32(b[off:off+4], v) }
	putU32(0, socketClientHello)
	putU32(4, uint32(n))
	putU32(8, 1)
	off := 12
	putU32(off, uint32(len(cfg.Cluster)))
	off += 4
	copy(b[off:], cfg.Cluster)
	off += len(cfg.Cluster)
	putU32(off, uint32(len(cfg.PBD)))
	off += 4
	copy(b[off:], cfg.PBD)
	off += len(cfg.PBD)
	putU32(off, uint32(cfg.HostID))
	putU32(off+4, uint32(cfg.Flags))
	// No optional capabilities are needed for the baseline registered-buffer protocol.
	binary.LittleEndian.PutUint64(b[off+8:], 0)
	return b
}

func encodeRegisterBuffers(descs []bufferDesc) []byte {
	b := make([]byte, 16+16*len(descs))
	binary.LittleEndian.PutUint32(b[0:], socketRegisterBuffers)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)))
	binary.LittleEndian.PutUint32(b[8:], 1)
	for i, d := range descs {
		off := 16 + i*16
		binary.LittleEndian.PutUint64(b[off:], d.id)
		binary.LittleEndian.PutUint64(b[off+8:], d.size)
	}
	return b
}

func decodeBufferMessage(b []byte) ([]bufferDesc, bool, error) {
	if len(b) < 16 || (len(b)-16)%16 != 0 {
		return nil, false, errors.New("malformed PFSD buffer message")
	}
	d := make([]bufferDesc, 0, (len(b)-16)/16)
	for off := 16; off < len(b); off += 16 {
		d = append(d, bufferDesc{binary.LittleEndian.Uint64(b[off:]), binary.LittleEndian.Uint64(b[off+8:])})
	}
	return d, binary.LittleEndian.Uint32(b[12:16]) != 0, nil
}

func (t *socketTransport) execute(ctx context.Context, requestType int32, payload []byte, bufferSize int, fill func([]byte)) ([]byte, []byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, nil, os.ErrClosed
	}
	if bufferSize > len(t.io) || len(payload) > len(t.io) {
		return nil, nil, fmt.Errorf("PFSD request payload is too large: %d bytes", bufferSize)
	}
	clear(t.request)
	// Path operations consume NUL-terminated strings from the shared buffer.
	// Clear the complete reusable mapping so a shorter request cannot retain a
	// suffix from the preceding one.
	clear(t.io)
	copy(t.io, payload)
	legacy := make([]byte, requestSize)
	putInt32(legacy, requestTypeOffset, requestType)
	if fill != nil {
		fill(legacy)
	}
	putInt32(t.request, 0, 1)
	putUint64(t.request, 8, 0)
	putUint64(t.request, 16, uint64(bufferSize))
	putInt32(t.request, 24, requestType)
	t.request[32] = 1
	copy(t.request[socketRequestUnion:socketResponseUnion], legacy[64:])
	putInt32(t.request, socketRequestUnion+4, int32(os.Getpid()))

	rp := make([]byte, 32)
	putUint64(rp, 0, t.connectionID)
	putUint64(rp, 8, 0)
	putUint64(rp, 16, socketRequestSize)
	putInt32(rp, 24, 0)
	if err := enqueue(ctx, t.queues[0], rp); err != nil {
		return nil, nil, err
	}
	for atomic.LoadUint32((*uint32)(unsafe.Pointer(&t.request[socketSemOffset]))) == 0 {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("wait for PFSD response: %w", ctx.Err())
		case <-time.After(50 * time.Microsecond):
		}
	}
	response := make([]byte, responseSize)
	copy(response[32:], t.request[socketResponseUnion:socketResponseUnion+socketResponseSize])
	data := append([]byte(nil), t.io[:bufferSize]...)
	return response, data, nil
}

func enqueue(ctx context.Context, q, value []byte) error {
	if len(q) < queueSlotsOffset+queueSlotSize {
		return errors.New("PFSD queue mapping is truncated")
	}
	capacity := atomic.LoadUint64((*uint64)(unsafe.Pointer(&q[queueCapacityOffset])))
	if capacity == 0 || capacity > uint64((len(q)-queueSlotsOffset)/queueSlotSize) {
		return fmt.Errorf("invalid PFSD queue capacity %d", capacity)
	}
	head := atomic.AddUint64((*uint64)(unsafe.Pointer(&q[queueHeadOffset])), 1) - 1
	slot := q[queueSlotsOffset+int(head%capacity)*queueSlotSize:]
	want := (head / capacity) * 2
	for atomic.LoadUint64((*uint64)(unsafe.Pointer(&slot[queueSlotTurnOffset]))) != want {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for PFSD queue slot: %w", ctx.Err())
		case <-time.After(50 * time.Microsecond):
		}
	}
	copy(slot[queueSlotValueOffset:queueSlotValueOffset+32], value)
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&slot[queueSlotTurnOffset])), want+1)
	// EventCount::notifyAll increments the high 32-bit epoch and wakes waiters.
	event := (*uint64)(unsafe.Pointer(&slot[queueSlotEmptyOffset]))
	prev := atomic.AddUint64(event, uint64(1)<<32) - (uint64(1) << 32)
	if uint32(prev) != 0 {
		_, _, _ = syscall.Syscall6(syscall.SYS_FUTEX, uintptr(unsafe.Pointer(&slot[queueSlotEmptyOffset+4])), futexWakeBitset, ^uintptr(0)>>1, 0, 0, ^uintptr(0))
	}
	return nil
}

func (t *socketTransport) readMessage(ctx context.Context) (uint32, []byte, []int, error) {
	for len(t.readBuf) < 8 || len(t.readBuf) < int(binary.LittleEndian.Uint32(t.readBuf[4:8])) {
		if deadline, ok := ctx.Deadline(); ok {
			_ = t.conn.SetReadDeadline(deadline)
		}
		buf := make([]byte, 64*1024)
		oob := make([]byte, unix.CmsgSpace(253*4))
		n, oobn, _, _, err := t.conn.ReadMsgUnix(buf, oob)
		if err != nil {
			return 0, nil, nil, err
		}
		if n == 0 {
			return 0, nil, nil, ioEOF
		}
		t.readBuf = append(t.readBuf, buf[:n]...)
		msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
		if err != nil {
			return 0, nil, nil, err
		}
		for _, m := range msgs {
			fds, err := unix.ParseUnixRights(&m)
			if err != nil {
				return 0, nil, nil, err
			}
			t.queuedFDs = append(t.queuedFDs, fds...)
		}
	}
	length := int(binary.LittleEndian.Uint32(t.readBuf[4:8]))
	if length < 8 || length > len(t.readBuf) {
		return 0, nil, nil, errors.New("invalid PFSD message length")
	}
	msg := append([]byte(nil), t.readBuf[:length]...)
	t.readBuf = t.readBuf[length:]
	fds := t.queuedFDs
	t.queuedFDs = nil
	return binary.LittleEndian.Uint32(msg), msg, fds, nil
}

var ioEOF = errors.New("PFSD socket closed")

func (t *socketTransport) writeMessage(ctx context.Context, b []byte, fds []int) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = t.conn.SetWriteDeadline(deadline)
	}
	oob := []byte(nil)
	if len(fds) != 0 {
		oob = unix.UnixRights(fds...)
	}
	n, _, err := t.conn.WriteMsgUnix(b, oob, nil)
	if err != nil {
		return fmt.Errorf("write PFSD message: %w", err)
	}
	if n != len(b) {
		return fmt.Errorf("short PFSD message write: %d of %d", n, len(b))
	}
	return nil
}

func createMemFD(name string, size int) (int, []byte, error) {
	fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return -1, nil, fmt.Errorf("create PFSD shared buffer: %w", err)
	}
	if err = unix.Ftruncate(fd, int64(size)); err != nil {
		_ = unix.Close(fd)
		return -1, nil, err
	}
	b, err := mapFD(fd, uint64(size))
	if err != nil {
		_ = unix.Close(fd)
		return -1, nil, err
	}
	return fd, b, nil
}

func mapFD(fd int, size uint64) ([]byte, error) {
	if size == 0 || size > uint64(^uint(0)>>1) {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("invalid shared buffer size %d", size)
	}
	b, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	unix.CloseOnExec(fd)
	return b, err
}

func closeFDs(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}

func (t *socketTransport) close() error {
	if t == nil || t.closed {
		return nil
	}
	t.closed = true
	var errs []error
	if t.conn != nil {
		errs = append(errs, t.conn.Close())
	}
	for _, q := range t.queues {
		errs = append(errs, unix.Munmap(q))
	}
	if t.request != nil {
		errs = append(errs, unix.Munmap(t.request))
	}
	if t.io != nil {
		errs = append(errs, unix.Munmap(t.io))
	}
	if t.requestFD >= 0 {
		errs = append(errs, unix.Close(t.requestFD))
	}
	if t.ioFD >= 0 {
		errs = append(errs, unix.Close(t.ioFD))
	}
	return errors.Join(errs...)
}

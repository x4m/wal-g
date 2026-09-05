//go:build linux && amd64

package pfsnative

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	maxPathLen  = 4096
	maxRequests = 64

	channelHeaderOffset  = 4096
	channelFreeOffset    = 576
	channelRequests      = 640
	channelResponses     = 70272
	channelBuffers       = 106496
	requestSize          = 1088
	responseSize         = 544
	requestValueOffset   = 64
	requestMountIDOffset = 128
	requestTypeOffset    = 132
	responseErrorOffset  = 36
	responseStatOffset   = 160
	responseStatResult   = 304
	requestPayloadOffset = 320
	requestCommonOffset  = 136
	responseCommonOffset = 40

	requestAlloc       = 1
	requestWaitReply   = 2
	requestWaitRelease = 4
	requestZombie      = 5

	requestOpen     = 1
	requestRead     = 2
	requestWrite    = 3
	requestUnlink   = 6
	requestStat     = 8
	requestMkdir    = 11
	requestRmdir    = 12
	requestOpenDir  = 13
	requestReadDir  = 14
	requestRename   = 16
	responseOpen    = 1001
	responseRead    = 1002
	responseWrite   = 1003
	responseUnlink  = 1006
	responseStat    = 1008
	responseMkdir   = 1011
	responseRmdir   = 1012
	responseOpenDir = 1013
	responseReadDir = 1014
	responseRename  = 1016
)

type FileInfo struct {
	path    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
}

func (i FileInfo) Name() string       { return filepath.Base(i.path) }
func (i FileInfo) Size() int64        { return i.size }
func (i FileInfo) Mode() fs.FileMode  { return i.mode }
func (i FileInfo) ModTime() time.Time { return i.modTime }
func (i FileInfo) IsDir() bool        { return i.mode.IsDir() }
func (i FileInfo) Sys() any           { return nil }

// Stat executes the first native-Go filesystem RPC supported by this
// prototype. Paths use the PFSD SDK form, for example /vdb/backups/file.
func (c *Client) Stat(ctx context.Context, path string) (FileInfo, error) {
	if err := validateCString("path", path, maxPathLen); err != nil {
		return FileInfo{}, err
	}
	for attempts := 0; attempts < 2; attempts++ {
		response, err := c.call(ctx, requestStat, []byte(path))
		if err != nil {
			return FileInfo{}, err
		}
		errno := syscall.Errno(int32At(response, responseErrorOffset))
		if errno == syscall.ESTALE && attempts == 0 {
			continue
		}
		if result := int32At(response, responseStatResult); result != 0 {
			if errno == 0 {
				errno = syscall.EIO
			}
			return FileInfo{}, &RPCError{Op: "stat", Path: path, Err: errno}
		}
		if int32At(response, 32) != responseStat {
			return FileInfo{}, fmt.Errorf("unexpected PFSD response type: got %d, want %d", int32At(response, 32), responseStat)
		}
		st := response[responseStatOffset:]
		return FileInfo{
			path:    path,
			size:    int64At(st, 48),
			mode:    linuxMode(uint32At(st, 24)),
			modTime: time.Unix(int64At(st, 88), int64At(st, 96)),
		}, nil
	}
	panic("unreachable")
}

type RPCError struct {
	Op        string
	Path      string
	Err       error
	Temporary bool
	Ambiguous bool
}

func (e *RPCError) Error() string { return fmt.Sprintf("PFSD %s %q: %v", e.Op, e.Path, e.Err) }
func (e *RPCError) Unwrap() error { return e.Err }

func IsTemporary(err error) bool {
	var rpcErr *RPCError
	return errors.As(err, &rpcErr) && rpcErr.Temporary
}

func IsAmbiguous(err error) bool {
	var rpcErr *RPCError
	return errors.As(err, &rpcErr) && rpcErr.Ambiguous
}

type File struct {
	client *Client
	path   string
	inode  int64
	offset int64
	common [16]byte
	flags  int

	mu     sync.Mutex
	closed bool
}

func (c *Client) Open(ctx context.Context, path string) (*File, error) {
	return c.OpenFile(ctx, path, syscall.O_RDONLY, 0)
}

func (c *Client) OpenFile(ctx context.Context, path string, flags int, mode uint32) (*File, error) {
	if err := validateCString("path", path, maxPathLen); err != nil {
		return nil, err
	}
	for attempts := 0; attempts < 2; attempts++ {
		response, _, err := c.execute(ctx, requestOpen, []byte(path), len(path), func(request []byte) {
			putInt32(request, requestPayloadOffset, int32(flags))
			putUint32(request, requestPayloadOffset+4, mode)
		})
		if err != nil {
			if flags&(syscall.O_CREAT|syscall.O_TRUNC) != 0 {
				return nil, mutatingError("open", path, err)
			}
			return nil, err
		}
		errno := syscall.Errno(int32At(response, responseErrorOffset))
		if errno == syscall.ESTALE && attempts == 0 && flags&(syscall.O_CREAT|syscall.O_TRUNC) == 0 {
			continue
		}
		if int32At(response, 32) != responseOpen {
			err = fmt.Errorf("unexpected PFSD response type: got %d, want %d", int32At(response, 32), responseOpen)
			if flags&(syscall.O_CREAT|syscall.O_TRUNC) != 0 {
				return nil, &RPCError{Op: "open", Path: path, Err: err, Ambiguous: true}
			}
			return nil, err
		}
		inode := int64At(response, 160)
		if inode < 0 {
			if errno == 0 {
				errno = syscall.EIO
			}
			return nil, classifiedRPCError("open", path, errno, flags&(syscall.O_CREAT|syscall.O_TRUNC) != 0)
		}
		f := &File{client: c, path: path, inode: inode, offset: int64At(response, 168), flags: flags}
		copy(f.common[:], response[responseCommonOffset:responseCommonOffset+len(f.common)])
		return f, nil
	}
	panic("unreachable")
}

func (f *File) ReadContext(ctx context.Context, b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if len(b) == 0 {
		return 0, nil
	}
	if len(b) > MaxIOSize {
		b = b[:MaxIOSize]
	}

	for attempts := 0; attempts < 2; attempts++ {
		response, data, err := f.client.execute(ctx, requestRead, nil, len(b), func(request []byte) {
			copy(request[requestCommonOffset:requestCommonOffset+len(f.common)], f.common[:])
			putInt64(request, requestPayloadOffset, f.inode)
			putUint64(request, requestPayloadOffset+8, uint64(len(b)))
			putInt64(request, requestPayloadOffset+16, f.offset)
		})
		if err != nil {
			return 0, err
		}
		errno := syscall.Errno(int32At(response, responseErrorOffset))
		if errno == syscall.ESTALE && attempts == 0 {
			continue
		}
		if int32At(response, 32) != responseRead {
			return 0, fmt.Errorf("unexpected PFSD response type: got %d, want %d", int32At(response, 32), responseRead)
		}
		n := int64At(response, 168)
		if n < 0 {
			if errno == 0 {
				errno = syscall.EIO
			}
			return 0, &RPCError{Op: "read", Path: f.path, Err: errno}
		}
		if n > int64(len(b)) || n > int64(len(data)) {
			return 0, errors.New("PFSD returned an invalid read length")
		}
		copy(b, data[:n])
		f.offset += n
		if n == 0 {
			return 0, io.EOF
		}
		return int(n), nil
	}
	panic("unreachable")
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (c *Client) call(ctx context.Context, requestType int32, payload []byte) ([]byte, error) {
	response, _, err := c.execute(ctx, requestType, payload, len(payload), nil)
	return response, err
}

func (c *Client) execute(ctx context.Context, requestType int32, payload []byte, bufferSize int, fill func([]byte)) ([]byte, []byte, error) {
	c.opMu.RLock()
	defer c.opMu.RUnlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, os.ErrClosed
	}
	mapping, header, err := selectMapping(c.mappings, bufferSize)
	if err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}
	connectionID, mountID := c.ack.ConnectID, c.ack.MountID
	c.mu.Unlock()

	channel, channelRequestsCount, err := firstChannel(mapping, header)
	if err != nil {
		return nil, nil, err
	}
	slot, request, response, buffer, err := allocateRequest(channel, channelRequestsCount, header.UnitSize, connectionID, mountID)
	if err != nil {
		return nil, nil, err
	}
	owned := true
	defer func() {
		if owned {
			releaseRequest(channel, request, slot)
		}
	}()

	for i := range buffer {
		buffer[i] = 0
	}
	copy(buffer, payload)
	putInt32(request, requestTypeOffset, requestType)
	if fill != nil {
		fill(request)
	}
	if !changeRequestState(request, requestAlloc, requestWaitReply) {
		return nil, nil, errors.New("PFSD request changed owner before send")
	}

	ticker := time.NewTicker(50 * time.Microsecond)
	defer ticker.Stop()
	for requestState(loadUint64(request, requestValueOffset)) != requestWaitRelease {
		select {
		case <-ctx.Done():
			markRequestZombie(request)
			owned = false
			return nil, nil, fmt.Errorf("wait for PFSD response: %w", ctx.Err())
		case <-ticker.C:
		}
	}
	result := append([]byte(nil), response...)
	data := append([]byte(nil), buffer[:bufferSize]...)
	releaseRequest(channel, request, slot)
	owned = false
	return result, data, nil
}

func selectMapping(mappings [][]byte, payloadLen int) ([]byte, shmHeader, error) {
	for _, mapping := range mappings {
		h, err := decodeShmHeader(mapping)
		if err != nil {
			return nil, shmHeader{}, err
		}
		if h.UnitSize >= uint64(payloadLen) {
			return mapping, h, nil
		}
	}
	return nil, shmHeader{}, fmt.Errorf("PFSD request payload is too large: %d bytes", payloadLen)
}

func firstChannel(mapping []byte, h shmHeader) ([]byte, int, error) {
	if h.Channels <= 0 || h.UnitSize > math.MaxInt || h.Size <= 0 || int64(h.Size) > int64(len(mapping)) {
		return nil, 0, errors.New("invalid PFSD shared memory dimensions")
	}
	if channelHeaderOffset+32 > len(mapping) {
		return nil, 0, errors.New("PFSD shared memory channel header is truncated")
	}
	channelRequestsCount := int(int32At(mapping[channelHeaderOffset:], 24))
	if channelRequestsCount <= 0 || channelRequestsCount > maxRequests {
		return nil, 0, fmt.Errorf("invalid PFSD request slot count: %d", channelRequestsCount)
	}
	stride := channelBuffers + channelRequestsCount*int(h.UnitSize)
	if channelHeaderOffset+stride > len(mapping) {
		return nil, 0, fmt.Errorf("PFSD shared memory channel is truncated: size=%d unit=%d requests=%d", len(mapping), h.UnitSize, channelRequestsCount)
	}
	ch := mapping[channelHeaderOffset : channelHeaderOffset+stride]
	if uint32At(ch, 4) != shmMagic || uint64At(ch, 16) != h.UnitSize {
		return nil, 0, errors.New("invalid PFSD shared memory channel header")
	}
	return ch, channelRequestsCount, nil
}

func allocateRequest(channel []byte, requestCount int, unitSize uint64, connectionID, mountID int32) (int, []byte, []byte, []byte, error) {
	bitmap := uint64Pointer(channel, channelFreeOffset)
	for slot := 0; slot < requestCount; slot++ {
		request := channel[channelRequests+slot*requestSize : channelRequests+(slot+1)*requestSize]
		value := uint64(uint16(connectionID)) | uint64(requestAlloc)<<16 | uint64(uint32(os.Getpid()))<<32
		valuePointer := uint64Pointer(request, requestValueOffset)
		oldValue := atomic.LoadUint64(valuePointer)
		if requestState(oldValue) != 0 || !atomic.CompareAndSwapUint64(valuePointer, oldValue, value) {
			continue
		}
		for {
			old := atomic.LoadUint64(bitmap)
			if atomic.CompareAndSwapUint64(bitmap, old, old&^(uint64(1)<<slot)) {
				break
			}
		}
		putUint32(request, 0, uint32At(channel, 0))
		putInt32(request, requestMountIDOffset, mountID)
		response := channel[channelResponses+slot*responseSize : channelResponses+(slot+1)*responseSize]
		clear(response[:32]) // glibc x86_64 process-shared sem_t representation
		start := channelBuffers + slot*int(unitSize)
		return slot, request, response, channel[start : start+int(unitSize)], nil
	}
	return 0, nil, nil, nil, errors.New("PFSD channel has no free request slots")
}

func releaseRequest(channel, request []byte, slot int) {
	bitmap := uint64Pointer(channel, channelFreeOffset)
	for {
		old := atomic.LoadUint64(bitmap)
		if atomic.CompareAndSwapUint64(bitmap, old, old|(uint64(1)<<slot)) {
			break
		}
	}
	_ = changeRequestState(request, requestWaitRelease, 0)
}

func changeRequestState(request []byte, from, to uint8) bool {
	p := uint64Pointer(request, requestValueOffset)
	for {
		old := atomic.LoadUint64(p)
		if requestState(old) != from {
			return false
		}
		updated := (old &^ (uint64(0xff) << 16)) | uint64(to)<<16
		if atomic.CompareAndSwapUint64(p, old, updated) {
			return true
		}
	}
}

func markRequestZombie(request []byte) {
	p := uint64Pointer(request, requestValueOffset)
	for {
		old := atomic.LoadUint64(p)
		state := requestState(old)
		if state == requestWaitRelease || state == requestZombie {
			return
		}
		updated := (old &^ (uint64(0xff) << 16)) | uint64(requestZombie)<<16
		if atomic.CompareAndSwapUint64(p, old, updated) {
			return
		}
	}
}

func requestState(value uint64) uint8 { return uint8(value >> 16) }

func uint64Pointer(b []byte, off int) *uint64 {
	return (*uint64)(unsafe.Pointer(&b[off]))
}

func loadUint64(b []byte, off int) uint64   { return atomic.LoadUint64(uint64Pointer(b, off)) }
func uint32At(b []byte, off int) uint32     { return binary.LittleEndian.Uint32(b[off:]) }
func int32At(b []byte, off int) int32       { return int32(uint32At(b, off)) }
func int64At(b []byte, off int) int64       { return int64(binary.LittleEndian.Uint64(b[off:])) }
func putUint32(b []byte, off int, v uint32) { binary.LittleEndian.PutUint32(b[off:], v) }
func putInt32(b []byte, off int, v int32)   { putUint32(b, off, uint32(v)) }
func putUint64(b []byte, off int, v uint64) { binary.LittleEndian.PutUint64(b[off:], v) }
func putInt64(b []byte, off int, v int64)   { putUint64(b, off, uint64(v)) }

func linuxMode(mode uint32) fs.FileMode {
	result := fs.FileMode(mode & 0o777)
	switch mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		result |= fs.ModeDir
	case syscall.S_IFLNK:
		result |= fs.ModeSymlink
	case syscall.S_IFIFO:
		result |= fs.ModeNamedPipe
	case syscall.S_IFSOCK:
		result |= fs.ModeSocket
	case syscall.S_IFCHR:
		result |= fs.ModeDevice | fs.ModeCharDevice
	case syscall.S_IFBLK:
		result |= fs.ModeDevice
	}
	return result
}

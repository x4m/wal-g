//go:build linux && amd64

package pfsnative

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"syscall"
)

const (
	direntBufferSize = 20 * 1024
	direntSize       = 280
	direntNameOffset = 19
)

type DirEntry struct {
	Name string
	FileInfo
}

func (f *File) Name() string { return f.path }

func (f *File) Read(b []byte) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), f.client.timeout)
	defer cancel()
	return f.ReadContext(ctx, b)
}

func (f *File) Write(b []byte) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), f.client.timeout)
	defer cancel()
	return f.WriteContext(ctx, b)
}

func (f *File) WriteContext(ctx context.Context, b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, syscall.EBADF
	}
	if len(b) == 0 {
		return 0, nil
	}
	if f.flags&(syscall.O_WRONLY|syscall.O_RDWR) == 0 {
		return 0, syscall.EBADF
	}

	f.client.rpcMu.Lock()
	defer f.client.rpcMu.Unlock()
	written := 0
	for written < len(b) {
		chunk := b[written:]
		if len(chunk) > MaxIOSize {
			chunk = chunk[:MaxIOSize]
		}
		offset := f.offset
		if f.flags&syscall.O_APPEND != 0 {
			offset = -2
		}
		response, _, err := f.client.execute(ctx, requestWrite, chunk, len(chunk), func(request []byte) {
			copy(request[requestCommonOffset:requestCommonOffset+len(f.common)], f.common[:])
			putInt64(request, requestPayloadOffset, f.inode)
			putInt64(request, requestPayloadOffset+8, offset)
			putUint64(request, requestPayloadOffset+16, uint64(len(chunk)))
			putInt32(request, requestPayloadOffset+24, int32(f.flags))
		})
		if err != nil {
			return written, mutatingError("write", f.path, err)
		}
		if got := int32At(response, 32); got != responseWrite {
			return written, &RPCError{Op: "write", Path: f.path, Err: fmt.Errorf("unexpected response type %d", got), Ambiguous: true}
		}
		n := int64At(response, 168)
		if n < 0 {
			return written, classifiedRPCError("write", f.path, responseErrno(response), true)
		}
		if n != int64(len(chunk)) {
			return written + int(n), &RPCError{Op: "write", Path: f.path, Err: io.ErrShortWrite, Ambiguous: true}
		}
		if f.flags&syscall.O_APPEND != 0 {
			f.offset = int64At(response, 176)
		} else {
			f.offset += n
		}
		written += int(n)
	}
	return written, nil
}

func (c *Client) Mkdir(ctx context.Context, name string, mode uint32) error {
	return c.pathMutation(ctx, "mkdir", name, requestMkdir, responseMkdir, func(request []byte) {
		putUint32(request, requestPayloadOffset, mode)
	})
}

func (c *Client) Unlink(ctx context.Context, name string) error {
	return c.pathMutation(ctx, "unlink", name, requestUnlink, responseUnlink, nil)
}

func (c *Client) Rmdir(ctx context.Context, name string) error {
	return c.pathMutation(ctx, "rmdir", name, requestRmdir, responseRmdir, nil)
}

func (c *Client) Remove(ctx context.Context, name string) error {
	info, err := c.Stat(ctx, name)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return c.Rmdir(ctx, name)
	}
	return c.Unlink(ctx, name)
}

func (c *Client) Rename(ctx context.Context, oldName, newName string) error {
	if err := validateCString("old path", oldName, maxPathLen); err != nil {
		return err
	}
	if err := validateCString("new path", newName, maxPathLen); err != nil {
		return err
	}
	payload := make([]byte, 2*maxPathLen)
	copy(payload, oldName)
	copy(payload[maxPathLen:], newName)
	c.rpcMu.Lock()
	defer c.rpcMu.Unlock()
	response, _, err := c.execute(ctx, requestRename, payload, len(payload), nil)
	if err != nil {
		return mutatingError("rename", oldName+" -> "+newName, err)
	}
	if got := int32At(response, 32); got != responseRename {
		return &RPCError{Op: "rename", Path: oldName, Err: fmt.Errorf("unexpected response type %d", got), Ambiguous: true}
	}
	if errno := responseErrno(response); errno != 0 {
		return classifiedRPCError("rename", oldName, errno, true)
	}
	return nil
}

func (c *Client) MkdirAll(ctx context.Context, name string, mode uint32) error {
	if path.Clean(name) == c.pbdRoot {
		return nil
	}
	info, err := c.Stat(ctx, name)
	if err == nil {
		if info.IsDir() {
			return nil
		}
		return fmt.Errorf("PFS path %q is not a directory", name)
	}
	if !errors.Is(err, syscall.ENOENT) {
		return err
	}
	parent := path.Dir(name)
	if parent != name {
		if err = c.MkdirAll(ctx, parent, mode); err != nil {
			return err
		}
	}
	err = c.Mkdir(ctx, name, mode)
	if errors.Is(err, syscall.EEXIST) {
		return nil
	}
	return err
}

func (c *Client) ReadDir(ctx context.Context, name string) ([]DirEntry, error) {
	if err := validateCString("path", name, maxPathLen); err != nil {
		return nil, err
	}
	c.rpcMu.Lock()
	response, _, err := c.execute(ctx, requestOpenDir, []byte(name), len(name), nil)
	if err != nil {
		c.rpcMu.Unlock()
		return nil, err
	}
	if got := int32At(response, 32); got != responseOpenDir {
		c.rpcMu.Unlock()
		return nil, fmt.Errorf("unexpected PFSD response type: got %d, want %d", got, responseOpenDir)
	}
	if int32At(response, 160) != 0 {
		c.rpcMu.Unlock()
		return nil, &RPCError{Op: "opendir", Path: name, Err: responseErrno(response)}
	}
	dirInode, nextInode := int64At(response, 168), int64At(response, 176)
	var nextOffset uint64
	var names []string
	for nextInode != 0 {
		response, data, callErr := c.execute(ctx, requestReadDir, nil, direntBufferSize, func(request []byte) {
			putInt64(request, requestPayloadOffset, dirInode)
			putInt64(request, requestPayloadOffset+8, nextInode)
			putUint64(request, requestPayloadOffset+16, nextOffset)
		})
		if callErr != nil {
			c.rpcMu.Unlock()
			return nil, callErr
		}
		result := int32At(response, 160)
		if result == 1 { // PFSD_DIR_END
			break
		}
		if result != 0 {
			c.rpcMu.Unlock()
			return nil, &RPCError{Op: "readdir", Path: name, Err: responseErrno(response)}
		}
		dataSize := uint64At(response, 184)
		if dataSize > uint64(len(data)) || dataSize%direntSize != 0 {
			c.rpcMu.Unlock()
			return nil, fmt.Errorf("PFSD returned invalid directory data size %d", dataSize)
		}
		for offset := 0; offset < int(dataSize); offset += direntSize {
			child := cString(data[offset+direntNameOffset : offset+direntSize])
			if child != "" && child != "." && child != ".." {
				names = append(names, child)
			}
		}
		nextInode = int64At(response, 168)
		nextOffset = uint64At(response, 176)
	}
	c.rpcMu.Unlock()

	entries := make([]DirEntry, 0, len(names))
	for _, child := range names {
		info, statErr := c.Stat(ctx, path.Join(name, child))
		if statErr != nil {
			return nil, statErr
		}
		entries = append(entries, DirEntry{Name: child, FileInfo: info})
	}
	return entries, nil
}

func (c *Client) pathMutation(ctx context.Context, op, name string, requestType, responseType int32, fill func([]byte)) error {
	if err := validateCString("path", name, maxPathLen); err != nil {
		return err
	}
	c.rpcMu.Lock()
	defer c.rpcMu.Unlock()
	response, _, err := c.execute(ctx, requestType, []byte(name), len(name), fill)
	if err != nil {
		return mutatingError(op, name, err)
	}
	if got := int32At(response, 32); got != responseType {
		return &RPCError{Op: op, Path: name, Err: fmt.Errorf("unexpected response type %d", got), Ambiguous: true}
	}
	if int32At(response, 160) != 0 {
		return classifiedRPCError(op, name, responseErrno(response), true)
	}
	return nil
}

func responseErrno(response []byte) syscall.Errno {
	return syscall.Errno(int32At(response, responseErrorOffset))
}

func mutatingError(op, name string, err error) error {
	return &RPCError{
		Op: op, Path: name, Err: err,
		Temporary: isTransient(err),
		Ambiguous: true,
	}
}

func classifiedRPCError(op, name string, err error, mutating bool) *RPCError {
	temporary := isTransient(err)
	return &RPCError{Op: op, Path: name, Err: err, Temporary: temporary, Ambiguous: mutating && temporary}
}

func isTransient(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, syscall.ESTALE) || errors.Is(err, syscall.EAGAIN) ||
		errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ENOTCONN)
}

func uint64At(b []byte, off int) uint64 { return uint64(int64At(b, off)) }

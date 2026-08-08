//go:build pfs && linux && cgo

package pfsclient

import (
	"errors"
	"fmt"
	"syscall"
)

// Error describes a failed PFS SDK operation.
// Temporary reports whether retrying the operation may succeed. Ambiguous is
// set when the server may have applied a mutating operation before the client
// observed the failure.
type Error struct {
	Op        string
	Err       error
	Temporary bool
	Ambiguous bool
	// RequiresRestart means the upstream process-global C SDK cannot be
	// safely reused after this connection failure.
	RequiresRestart bool
}

func (e *Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("PFS %s failed without setting errno", e.Op)
	}
	return fmt.Sprintf("PFS %s: %v", e.Op, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// IsTemporary reports whether err is a transient PFS connection or resource
// error. Callers must still check whether retrying their operation is safe.
func IsTemporary(err error) bool {
	var pfsErr *Error
	return errors.As(err, &pfsErr) && pfsErr.Temporary
}

// IsAmbiguous reports that a failed mutating operation may have reached PFS.
func IsAmbiguous(err error) bool {
	var pfsErr *Error
	return errors.As(err, &pfsErr) && pfsErr.Ambiguous
}

// RequiresProcessRestart reports that retry must happen in a fresh process.
func RequiresProcessRestart(err error) bool {
	var pfsErr *Error
	return errors.As(err, &pfsErr) && pfsErr.RequiresRestart
}

func classifyError(operation string, err error) *Error {
	result := &Error{Op: operation, Err: err}
	// Some pfsd SDK connection paths return -1 without preserving errno.
	// A bounded mount retry is safer than treating this as a filesystem error.
	if err == nil && operation == "mount" {
		result.Temporary = true
		result.RequiresRestart = true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EAGAIN, syscall.EINTR:
			result.Temporary = true
		case syscall.ETIMEDOUT, syscall.ECONNREFUSED, syscall.ECONNRESET,
			syscall.ENOTCONN, syscall.ENODEV:
			result.Temporary = true
			result.RequiresRestart = true
		}
	}
	switch operation {
	case "write", "rename", "unlink", "rmdir", "mkdir":
		result.Ambiguous = result.Temporary
	}
	return result
}

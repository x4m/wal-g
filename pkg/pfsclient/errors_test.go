//go:build pfs && linux && cgo

package pfsclient

import (
	"errors"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name, operation string
		err             error
		temporary       bool
		ambiguous       bool
		restart         bool
	}{
		{"read timeout", "read", syscall.ETIMEDOUT, true, false, true},
		{"write timeout", "write", syscall.ETIMEDOUT, true, true, true},
		{"try again", "stat", syscall.EAGAIN, true, false, false},
		{"invalid argument", "open", syscall.EINVAL, false, false, false},
		{"missing object", "stat", syscall.ENOENT, false, false, false},
		{"mount without errno", "mount", nil, true, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyError(tt.operation, tt.err)
			assert.Equal(t, tt.temporary, IsTemporary(err))
			assert.Equal(t, tt.ambiguous, IsAmbiguous(err))
			assert.Equal(t, tt.restart, RequiresProcessRestart(err))
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			}
		})
	}
}

func TestErrorSupportsErrorsAs(t *testing.T) {
	err := classifyError("mount", syscall.ECONNREFUSED)
	var pfsErr *Error
	require.True(t, errors.As(err, &pfsErr))
	assert.Equal(t, "mount", pfsErr.Op)
}

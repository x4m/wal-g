//go:build pfs && linux && cgo

package pfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePrefix(t *testing.T) {
	tests := []struct {
		name, prefix, root, device string
	}{
		{"absolute", "/nvme1n1/wal-g", "/nvme1n1/wal-g", "nvme1n1"},
		{"URL", "pfs://nvme1n1/wal-g", "/nvme1n1/wal-g", "nvme1n1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, device, err := parsePrefix(tt.prefix)
			require.NoError(t, err)
			assert.Equal(t, tt.root, root)
			assert.Equal(t, tt.device, device)
		})
	}
}

func TestParsePrefixRejectsEmptyPath(t *testing.T) {
	_, _, err := parsePrefix("")
	require.Error(t, err)
}

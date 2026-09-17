package postgres

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestEqualWALContents(t *testing.T) {
	for _, size := range []int{0, 1, 1024 * 1024, 1024*1024 + 1, 2 * 1024 * 1024} {
		payload := bytes.Repeat([]byte{'a'}, size)
		equal, err := equalWALContents(bytes.NewReader(payload), iotest.HalfReader(bytes.NewReader(payload)))
		require.NoError(t, err)
		require.True(t, equal)
		for _, other := range [][]byte{append(append([]byte(nil), payload...), 'b'), []byte("different")} {
			equal, err = equalWALContents(bytes.NewReader(payload), bytes.NewReader(other))
			require.NoError(t, err)
			require.False(t, equal)
		}
	}
	expected := errors.New("read failed")
	for _, reverse := range []bool{false, true} {
		var a, b io.Reader = iotest.ErrReader(expected), bytes.NewReader(nil)
		if reverse {
			a, b = b, a
		}
		_, err := equalWALContents(a, b)
		require.ErrorIs(t, err, expected)
	}
}

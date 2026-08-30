//go:build pfs && linux && cgo && integration

package pfsclient

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientRoundTrip requires a running pfsdaemon and a writable PFS_TEST_ROOT,
// for example /nvme1n1/wal-g-integration.
func TestClientRoundTrip(t *testing.T) {
	root := os.Getenv("PFS_TEST_ROOT")
	if root == "" {
		t.Skip("PFS_TEST_ROOT is not set")
	}
	parts := strings.Split(strings.Trim(root, "/"), "/")
	require.NotEmpty(t, parts[0])
	hostID := DefaultHostID
	if value := os.Getenv("PFS_TEST_HOST_ID"); value != "" {
		var err error
		hostID, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	client, err := Open(Config{
		Device: parts[0], Cluster: os.Getenv("PFS_TEST_CLUSTER"),
		HostID: hostID, Server: os.Getenv("PFS_TEST_SERVER"),
	})
	require.NoError(t, err)
	defer client.Close()

	testDir := path.Join(root, "run-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	require.NoError(t, client.MkdirAll(testDir))
	defer client.Remove(testDir)

	original := path.Join(testDir, "original")
	renamed := path.Join(testDir, "renamed")
	file, err := client.OpenFile(original, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	require.NoError(t, err)
	expected := bytes.Repeat([]byte("pfs round trip"), 256*1024)
	_, err = file.Write(expected)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, client.Rename(original, renamed))
	defer client.Remove(renamed)

	file, err = client.OpenFile(renamed, os.O_RDONLY, 0)
	require.NoError(t, err)
	contents, err := io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	assert.Equal(t, expected, contents)

	entries, err := client.ReadDir(testDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "renamed", entries[0].Name)
}

func TestOpenClassifiesUnavailableServer(t *testing.T) {
	root := os.Getenv("PFS_TEST_ROOT")
	if root == "" {
		t.Skip("PFS_TEST_ROOT is not set")
	}
	if os.Getenv("PFS_TEST_OPEN_HELPER") == "1" {
		parts := strings.Split(strings.Trim(root, "/"), "/")
		_, err := Open(Config{
			Device: parts[0], Cluster: os.Getenv("PFS_TEST_CLUSTER"), HostID: 127,
			Server: os.Getenv("PFS_TEST_MISSING_SERVER"), Timeout: 100 * time.Millisecond,
		})
		require.Error(t, err)
		assert.True(t, IsTemporary(err))
		assert.True(t, RequiresProcessRestart(err))
		return
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestOpenClassifiesUnavailableServer$", "-test.v")
	cmd.Env = append(os.Environ(), "PFS_TEST_OPEN_HELPER=1", "PFS_TEST_MISSING_SERVER="+t.TempDir())
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

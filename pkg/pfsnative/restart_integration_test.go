//go:build linux && amd64

package pfsnative

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestSocketDaemonRestartIntegration never controls a daemon itself. A fixture
// restarts its disposable daemon between ready and restarted marker files.
func TestSocketDaemonRestartIntegration(t *testing.T) {
	signalDir := os.Getenv("PFSNATIVE_TEST_RESTART_SIGNAL_DIR")
	if signalDir == "" {
		t.Skip("no external daemon restart fixture")
	}
	statPath := os.Getenv("PFSNATIVE_TEST_STAT_PATH")
	if statPath == "" {
		t.Fatal("PFSNATIVE_TEST_STAT_PATH is required")
	}
	hostID, err := strconv.Atoi(os.Getenv("PFSNATIVE_TEST_HOST_ID"))
	if err != nil {
		t.Fatal("PFSNATIVE_TEST_HOST_ID must be an integer")
	}
	cfg := Config{PBD: os.Getenv("PFSNATIVE_TEST_PBD"), Cluster: os.Getenv("PFSNATIVE_TEST_CLUSTER"), ServerDir: os.Getenv("PFSNATIVE_TEST_SERVER_DIR"), HostID: hostID, Flags: ReadOnly, Timeout: 10 * time.Second}
	c, err := Mount(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.Stat(t.Context(), statPath); err != nil {
		t.Fatal(err)
	}
	// Mount may have to load on-disk metadata. Bound only the stale-session
	// request aggressively; do not impose that budget on a cold mount.
	c.timeout = 200 * time.Millisecond
	if err = os.WriteFile(filepath.Join(signalDir, "ready"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err = os.Stat(filepath.Join(signalDir, "restarted")); err == nil {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("fixture did not restart the daemon")
		case <-ticker.C:
		}
	}
	_, err = c.Stat(context.Background(), statPath)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale client: %v", err)
	}
	_, err = c.Stat(context.Background(), statPath)
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("stale client reused: %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	fresh, err := Mount(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if _, err = fresh.Stat(t.Context(), statPath); err != nil {
		t.Fatal(err)
	}
}

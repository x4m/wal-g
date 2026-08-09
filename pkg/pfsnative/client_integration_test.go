//go:build linux && amd64

package pfsnative

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestMountTimesOutWithoutDaemon(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := Mount(ctx, Config{ServerDir: dir, PBD: "missing", HostID: 1, Flags: ReadOnly})
	if err == nil {
		t.Fatal("expected mount to time out")
	}
	if removeErr := os.RemoveAll(dir); removeErr != nil {
		t.Fatalf("pidfile descriptor was leaked: %v", removeErr)
	}
}

func TestMountIntegration(t *testing.T) {
	pbd := os.Getenv("PFSNATIVE_TEST_PBD")
	if pbd == "" {
		t.Skip("PFSNATIVE_TEST_PBD is not set")
	}
	hostID, err := strconv.Atoi(os.Getenv("PFSNATIVE_TEST_HOST_ID"))
	if err != nil {
		t.Fatalf("PFSNATIVE_TEST_HOST_ID must be an integer: %v", err)
	}
	serverDir := os.Getenv("PFSNATIVE_TEST_SERVER_DIR")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Mount(ctx, Config{
		ServerDir: serverDir,
		PBD:       pbd,
		HostID:    hostID,
		Flags:     ReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mounted PBD %s: connection=%d mount=%d", pbd, c.ConnectionID(), c.MountID())
	statPath := os.Getenv("PFSNATIVE_TEST_STAT_PATH")
	if statPath == "" {
		statPath = "/" + pbd + "/wal-g"
	}
	info, err := c.Stat(ctx, statPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("PBD root mode is %v, expected a directory", info.Mode())
	}
	t.Logf("native stat %s: mode=%v size=%d", statPath, info.Mode(), info.Size())
	if readPath := os.Getenv("PFSNATIVE_TEST_READ_PATH"); readPath != "" {
		file, openErr := c.Open(ctx, readPath)
		if openErr != nil {
			t.Fatal(openErr)
		}
		buf := make([]byte, 4096)
		n, readErr := file.Read(ctx, buf)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if n == 0 {
			t.Fatal("native read returned no data")
		}
		if expected := os.Getenv("PFSNATIVE_TEST_READ_SHA256"); expected != "" {
			actual := sha256.Sum256(buf[:n])
			if hex.EncodeToString(actual[:]) != expected {
				t.Fatalf("native read SHA-256 = %x, want %s", actual, expected)
			}
		}
		t.Logf("native open/read %s: %d bytes", readPath, n)
		if closeErr := file.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if err = c.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
}

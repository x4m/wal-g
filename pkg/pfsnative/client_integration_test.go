//go:build linux && amd64

package pfsnative

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strconv"
	"syscall"
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

func TestFilesystemIntegration(t *testing.T) {
	root := os.Getenv("PFSNATIVE_TEST_MUTATION_ROOT")
	if root == "" {
		t.Skip("PFSNATIVE_TEST_MUTATION_ROOT is not set")
	}
	pbd := os.Getenv("PFSNATIVE_TEST_PBD")
	hostID, err := strconv.Atoi(os.Getenv("PFSNATIVE_TEST_HOST_ID"))
	if err != nil {
		t.Fatalf("PFSNATIVE_TEST_HOST_ID must be an integer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := Mount(ctx, Config{
		ServerDir: os.Getenv("PFSNATIVE_TEST_SERVER_DIR"),
		Cluster:   os.Getenv("PFSNATIVE_TEST_CLUSTER"),
		PBD:       pbd,
		HostID:    hostID,
		Flags:     ReadWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	dir := root + "/case-" + strconv.Itoa(os.Getpid())
	original, renamed := dir+"/original.bin", dir+"/renamed.bin"
	_ = c.Unlink(ctx, original)
	_ = c.Unlink(ctx, renamed)
	_ = c.Rmdir(ctx, dir)
	if err = c.MkdirAll(ctx, dir, 0o755); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("native-pfsd-go\x00"), 192*1024)
	f, err := c.OpenFile(ctx, original, syscall.O_CREAT|syscall.O_TRUNC|syscall.O_WRONLY, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if n, writeErr := f.WriteContext(ctx, payload); writeErr != nil || n != len(payload) {
		t.Fatalf("write = %d, %v", n, writeErr)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := c.Stat(ctx, original)
	if err != nil || info.Size() != int64(len(payload)) {
		t.Fatalf("stat = %+v, %v", info, err)
	}
	f, err = c.Open(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	actual := make([]byte, len(payload))
	if _, err = io.ReadFull(f, actual); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if !bytes.Equal(actual, payload) {
		t.Fatal("read data differs from written data")
	}
	if err = c.Rename(ctx, original, renamed); err != nil {
		t.Fatal(err)
	}
	entries, err := c.ReadDir(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "renamed.bin" || entries[0].Size() != int64(len(payload)) {
		t.Fatalf("unexpected directory entries: %+v", entries)
	}
	if err = c.Unlink(ctx, renamed); err != nil {
		t.Fatal(err)
	}
	if err = c.Rmdir(ctx, dir); err != nil {
		t.Fatal(err)
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
		Cluster:   os.Getenv("PFSNATIVE_TEST_CLUSTER"),
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
		n, readErr := file.ReadContext(ctx, buf)
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

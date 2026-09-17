//go:build linux && amd64

package pfsnative

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSocketMountUsesConfiguredTimeout(t *testing.T) {
	addr := &net.UnixAddr{Name: filepath.Join(t.TempDir(), "silent.socket"), Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		defer conn.Close()
		<-finished // Connected daemon never sends the handshake.
	}()
	_, err = Mount(context.Background(), Config{ServerDir: addr.Name, PBD: "disk", Flags: ReadOnly, Timeout: 20 * time.Millisecond})
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("want timeout, got %v", err)
	}
}

func TestMissingExplicitSocketDoesNotUsePidfile(t *testing.T) {
	_, err := Mount(context.Background(), Config{ServerDir: filepath.Join(t.TempDir(), "absent.socket"), PBD: "disk", Flags: ReadOnly})
	if err == nil || !strings.Contains(err.Error(), "connect to PFSD socket") {
		t.Fatalf("want socket connection error, got %v", err)
	}
}

func TestSocketQueueCancellationDoesNotReserveSlot(t *testing.T) {
	q := make([]byte, pageSize)
	putUint64(q, queueCapacityOffset, 1)
	putUint64(q, queueSlotsOffset+queueSlotTurnOffset, 1) // full
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := enqueue(ctx, q, make([]byte, 32)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("enqueue: %v", err)
	}
	if got := uint64At(q, queueHeadOffset); got != 0 {
		t.Fatalf("cancelled enqueue reserved head %d", got)
	}
	putUint64(q, queueSlotsOffset+queueSlotTurnOffset, 0)
	if err := enqueue(context.Background(), q, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
}

func TestSocketTimeoutNeverReusesInflightBuffers(t *testing.T) {
	q := make([]byte, pageSize)
	putUint64(q, queueCapacityOffset, 1)
	transport := &socketTransport{queues: [][]byte{q}, request: make([]byte, pageSize), io: make([]byte, pageSize)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, _, err := transport.execute(ctx, requestWrite, []byte("first"), 5, nil)
	if !errors.Is(err, context.DeadlineExceeded) || !IsAmbiguous(err) {
		t.Fatalf("want ambiguous deadline error, got %v", err)
	}
	requestBefore := append([]byte(nil), transport.request...)
	ioBefore := append([]byte(nil), transport.io...)
	_, _, err = transport.execute(context.Background(), requestWrite, []byte("second"), 6, nil)
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("want unusable connection, got %v", err)
	}
	if !bytes.Equal(requestBefore, transport.request) || !bytes.Equal(ioBefore, transport.io) {
		t.Fatal("in-flight buffers were reused")
	}
	// Even a late daemon response must not make this session usable again.
	putUint32(transport.request, socketSemOffset, 1)
	_, _, err = transport.execute(context.Background(), requestStat, nil, 0, nil)
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("late response revived session: %v", err)
	}
}

func TestSocketCancelledContextDoesNotPublish(t *testing.T) {
	q := make([]byte, pageSize)
	putUint64(q, queueCapacityOffset, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := enqueue(ctx, q, make([]byte, 32)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if uint64At(q, queueHeadOffset) != 0 {
		t.Fatal("published cancelled request")
	}
}

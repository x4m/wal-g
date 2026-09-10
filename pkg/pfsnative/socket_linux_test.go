//go:build linux && amd64

package pfsnative

import (
	"context"
	"encoding/binary"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestSocketTransportStat(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "pfsd-test.socket")
	addr := &net.UnixAddr{Name: socketPath, Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() { serverErr <- serveSocketStat(listener) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Mount(ctx, Config{ServerDir: socketPath, Cluster: "test", PBD: "disk", HostID: 7, Flags: ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.Stat(ctx, "/disk/data")
	if err != nil {
		select {
		case serverFailure := <-serverErr:
			t.Fatalf("client: %v; server: %v", err, serverFailure)
		default:
			t.Fatal(err)
		}
	}
	if info.Size() != 12345 || !info.IsDir() {
		t.Fatalf("unexpected stat: size=%d mode=%v", info.Size(), info.Mode())
	}
	if c.ConnectionID() != 42 {
		t.Fatalf("connection ID = %d, want 42", c.ConnectionID())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func serveSocketStat(listener *net.UnixListener) error {
	conn, err := listener.AcceptUnix()
	if err != nil {
		return err
	}
	defer conn.Close()
	queueFD, queue, err := createMemFD("test-pfs-queue", pageSize)
	if err != nil {
		return err
	}
	defer unix.Close(queueFD)
	defer unix.Munmap(queue)
	putUint64(queue, queueCapacityOffset, 4)
	queueMsg := make([]byte, 32)
	putUint32(queueMsg, 0, socketRegisterQueues)
	putUint32(queueMsg, 4, uint32(len(queueMsg)))
	putUint32(queueMsg, 8, 1)
	putUint32(queueMsg, 12, 1)
	putUint64(queueMsg, 16, 0)
	putUint64(queueMsg, 24, uint64(len(queue)))
	if _, _, err = conn.WriteMsgUnix(queueMsg, unix.UnixRights(queueFD), nil); err != nil {
		return err
	}
	if typ, _, _, err := readTestMessage(conn); err != nil || typ != socketClientHello {
		if err != nil {
			return err
		}
		return errorsNew("missing client hello")
	}
	hello := make([]byte, 24)
	putUint32(hello, 0, socketServerHello)
	putUint32(hello, 4, uint32(len(hello)))
	putUint32(hello, 8, 1)
	putUint64(hello, 12, 42)
	if _, err = conn.Write(hello); err != nil {
		return err
	}
	typ, msg, fds, err := readTestMessage(conn)
	if err != nil {
		return err
	}
	if typ != socketRegisterBuffers || len(fds) != 2 {
		closeFDs(fds)
		return errorsNew("invalid buffer registration")
	}
	descs, _, err := decodeBufferMessage(msg)
	if err != nil {
		closeFDs(fds)
		return err
	}
	request, err := mapFD(fds[0], descs[0].size)
	if err != nil {
		closeFDs(fds)
		return err
	}
	defer unix.Munmap(request)
	ioBuf, err := mapFD(fds[1], descs[1].size)
	closeFDs(fds)
	if err != nil {
		return err
	}
	defer unix.Munmap(ioBuf)
	ack := make([]byte, 8)
	putUint32(ack, 0, socketAck)
	putUint32(ack, 4, 8)
	if _, err = conn.Write(ack); err != nil {
		return err
	}

	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadUint64((*uint64)(unsafe.Pointer(&queue[queueSlotsOffset+queueSlotTurnOffset]))) != 1 {
		if time.Now().After(deadline) {
			return errorsNew("request was not enqueued")
		}
		time.Sleep(50 * time.Microsecond)
	}
	if got := int32At(request, 24); got != requestStat {
		return errorsNew("unexpected request type")
	}
	if string(ioBuf[:10]) != "/disk/data" {
		return errorsNew("unexpected request path")
	}
	rsp := request[socketResponseUnion:]
	putInt32(rsp, 0, responseStat)
	putInt32(rsp, 272, 0)
	putUint32(rsp, 128+24, unix.S_IFDIR|0o755)
	putInt64(rsp, 128+48, 12345)
	putInt64(rsp, 128+88, 1700000000)
	atomic.StoreUint32((*uint32)(unsafe.Pointer(&request[socketSemOffset])), 1)
	return nil
}

func readTestMessage(conn *net.UnixConn) (uint32, []byte, []int, error) {
	b := make([]byte, 64*1024)
	oob := make([]byte, unix.CmsgSpace(8*4))
	n, oobn, _, _, err := conn.ReadMsgUnix(b, oob)
	if err != nil {
		return 0, nil, nil, err
	}
	if n < 8 {
		return 0, nil, nil, errorsNew("short message")
	}
	length := int(binary.LittleEndian.Uint32(b[4:8]))
	if length > n {
		return 0, nil, nil, errorsNew("split test message")
	}
	var fds []int
	msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return 0, nil, nil, err
	}
	for _, m := range msgs {
		got, parseErr := unix.ParseUnixRights(&m)
		if parseErr != nil {
			return 0, nil, nil, parseErr
		}
		fds = append(fds, got...)
	}
	return binary.LittleEndian.Uint32(b), append([]byte(nil), b[:length]...), fds, nil
}

type testError string

func (e testError) Error() string { return string(e) }
func errorsNew(s string) error    { return testError(s) }

package pfsnative

import (
	"encoding/binary"
	"testing"
)

func TestMountRequestLayout(t *testing.T) {
	b, err := encodeMountRequest(mountRequest{Cluster: "cluster", PBD: "disk", HostID: 17, Flags: 0x13, Epoch: 42})
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != pageSize || binary.LittleEndian.Uint32(b[0:4]) != 2 || cString(b[4:68]) != "cluster" || cString(b[68:132]) != "disk" {
		t.Fatalf("unexpected encoded request header: %x", b[:144])
	}
	if got := int32(binary.LittleEndian.Uint32(b[132:136])); got != 17 {
		t.Fatalf("host ID = %d", got)
	}
	if got := binary.LittleEndian.Uint32(b[140:144]); got != 42 {
		t.Fatalf("epoch = %d", got)
	}
}

func TestMountAckLayout(t *testing.T) {
	b := make([]byte, pageSize)
	errorCode := int32(-5)
	binary.LittleEndian.PutUint32(b[ackErrorOffset:], uint32(errorCode))
	binary.LittleEndian.PutUint32(b[ackVersionOffset:], 2)
	copy(b[ackMessageOffset:], "failed")
	binary.LittleEndian.PutUint32(b[ackConnectIDOffset:], 9)
	copy(b[ackShmNamesOffset:], "/tmp/shm0")
	binary.LittleEndian.PutUint32(b[ackMountIDOffset:], 11)
	binary.LittleEndian.PutUint32(b[ackEpochOffset:], 13)
	binary.LittleEndian.PutUint32(b[ackFlagsOffset:], 0x13)

	a, err := decodeMountAck(b)
	if err != nil {
		t.Fatal(err)
	}
	if a.Error != -5 || a.Version != 2 || a.Message != "failed" || a.ConnectID != 9 || a.ShmNames[0] != "/tmp/shm0" || a.MountID != 11 || a.Epoch != 13 || a.Flags != 0x13 {
		t.Fatalf("unexpected acknowledgement: %+v", a)
	}
}

func TestShmHeaderLayout(t *testing.T) {
	b := make([]byte, pageSize)
	binary.LittleEndian.PutUint32(b[0:4], shmMagic)
	binary.LittleEndian.PutUint32(b[4:8], shmVersion)
	binary.LittleEndian.PutUint32(b[8:12], 3)
	binary.LittleEndian.PutUint32(b[12:16], 4096)
	binary.LittleEndian.PutUint64(b[16:24], 1<<20)
	binary.LittleEndian.PutUint32(b[24:28], 8)
	binary.LittleEndian.PutUint32(b[28:32], 2)

	h, err := decodeShmHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if h.Magic != shmMagic || h.Version != 2 || h.Epoch != 3 || h.Size != 4096 || h.UnitSize != 1<<20 || h.Channels != 8 || h.Index != 2 {
		t.Fatalf("unexpected shared memory header: %+v", h)
	}
}

func TestRejectsNonTerminatedNames(t *testing.T) {
	for _, value := range []string{string(make([]byte, maxNameLen)), "bad\x00name"} {
		if _, err := encodeMountRequest(mountRequest{Cluster: value, PBD: "disk"}); err == nil {
			t.Fatalf("expected %q to be rejected", value)
		}
	}
}

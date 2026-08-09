package pfsnative

import (
	"encoding/binary"
	"fmt"
	"strings"
)

const (
	protocolVersion = 2
	pageSize        = 4096
	pidFileSize     = 2 * pageSize
	maxNameLen      = 64
	maxShmFiles     = 7
	maxFileNameLen  = 512

	shmMagic   = 0x0133c96c
	shmVersion = 2
)

const (
	ackErrorOffset     = 0
	ackVersionOffset   = 4
	ackMessageOffset   = 8
	ackConnectIDOffset = 72
	ackShmNamesOffset  = 76
	ackMountIDOffset   = ackShmNamesOffset + maxShmFiles*maxFileNameLen
	ackEpochOffset     = ackMountIDOffset + 4
	ackFlagsOffset     = ackEpochOffset + 4
	ackRemountOffset   = ackFlagsOffset + 4
)

type mountRequest struct {
	Cluster string
	PBD     string
	HostID  int32
	Flags   int32
	Epoch   uint32
}

type mountAck struct {
	Error        int32
	Version      uint32
	Message      string
	ConnectID    int32
	ShmNames     [maxShmFiles]string
	MountID      int32
	Epoch        uint32
	Flags        int32
	RemountError int32
}

type shmHeader struct {
	Magic    uint32
	Version  uint32
	Epoch    uint32
	Size     int32
	UnitSize uint64
	Channels int32
	Index    int32
}

func encodeMountRequest(req mountRequest) ([]byte, error) {
	if err := validateCString("cluster", req.Cluster, maxNameLen); err != nil {
		return nil, err
	}
	if err := validateCString("PBD", req.PBD, maxNameLen); err != nil {
		return nil, err
	}

	b := make([]byte, pageSize)
	binary.LittleEndian.PutUint32(b[0:4], protocolVersion)
	copy(b[4:68], req.Cluster)
	copy(b[68:132], req.PBD)
	binary.LittleEndian.PutUint32(b[132:136], uint32(req.HostID))
	binary.LittleEndian.PutUint32(b[136:140], uint32(req.Flags))
	binary.LittleEndian.PutUint32(b[140:144], req.Epoch)
	return b, nil
}

func decodeMountAck(b []byte) (mountAck, error) {
	if len(b) != pageSize {
		return mountAck{}, fmt.Errorf("invalid PFSD mount acknowledgement size: got %d, want %d", len(b), pageSize)
	}

	a := mountAck{
		Error:        int32(binary.LittleEndian.Uint32(b[ackErrorOffset:])),
		Version:      binary.LittleEndian.Uint32(b[ackVersionOffset:]),
		Message:      cString(b[ackMessageOffset : ackMessageOffset+64]),
		ConnectID:    int32(binary.LittleEndian.Uint32(b[ackConnectIDOffset:])),
		MountID:      int32(binary.LittleEndian.Uint32(b[ackMountIDOffset:])),
		Epoch:        binary.LittleEndian.Uint32(b[ackEpochOffset:]),
		Flags:        int32(binary.LittleEndian.Uint32(b[ackFlagsOffset:])),
		RemountError: int32(binary.LittleEndian.Uint32(b[ackRemountOffset:])),
	}
	for i := range a.ShmNames {
		start := ackShmNamesOffset + i*maxFileNameLen
		a.ShmNames[i] = cString(b[start : start+maxFileNameLen])
	}
	return a, nil
}

func decodeShmHeader(b []byte) (shmHeader, error) {
	// pfsd_shm_t is a Linux 64-bit C ABI structure. size_t is aligned at 16.
	if len(b) < 36 {
		return shmHeader{}, fmt.Errorf("PFSD shared memory header is truncated: %d bytes", len(b))
	}
	return shmHeader{
		Magic:    binary.LittleEndian.Uint32(b[0:4]),
		Version:  binary.LittleEndian.Uint32(b[4:8]),
		Epoch:    binary.LittleEndian.Uint32(b[8:12]),
		Size:     int32(binary.LittleEndian.Uint32(b[12:16])),
		UnitSize: binary.LittleEndian.Uint64(b[16:24]),
		Channels: int32(binary.LittleEndian.Uint32(b[24:28])),
		Index:    int32(binary.LittleEndian.Uint32(b[28:32])),
	}, nil
}

func validateCString(name, value string, capacity int) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%s contains a NUL byte", name)
	}
	if len(value) >= capacity {
		return fmt.Errorf("%s is too long: got %d bytes, maximum is %d", name, len(value), capacity-1)
	}
	return nil
}

func cString(b []byte) string {
	if i := strings.IndexByte(string(b), 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

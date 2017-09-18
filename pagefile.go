package walg

/*
#include <inttypes.h>
typedef struct PageHeaderData
{
	uint64_t 		pd_lsn;

	uint16_t		pd_checksum;
	uint16_t		pd_flags;
	uint16_t		pd_lower;
	uint16_t 		pd_upper;
	uint16_t 		pd_special;
	uint16_t		pd_pagesize_version;
	uint32_t 		pd_prune_xid;
} PageHeaderData;

typedef struct PageProbeResult
{
	int success;
	uint64_t lsn;
} PageProbeResult;

#define valid_flags     (7)
#define invalid_lsn     (0)
#define layout_version  (4)
#define header_size     (24)
#define block_size		(8192)

PageProbeResult GetLSNIfPageIsValid(void* ptr)
{
	PageHeaderData* data = (PageHeaderData*) ptr;
	PageProbeResult result = {0 , invalid_lsn};

	if ((data->pd_flags & valid_flags) != data->pd_flags ||
		data->pd_lower < header_size ||
		data->pd_lower > data->pd_upper ||
		data->pd_upper > data->pd_special ||
		data->pd_special > block_size ||
		data->pd_lsn == invalid_lsn ||
		data->pd_pagesize_version != block_size + layout_version)
	{
		return result;
	}

	result.success = 1;
	result.lsn = ((PageHeaderData*) data)->pd_lsn;
	return result;
}
*/
import "C"
import (
	"unsafe"
	"os"
	"strings"
	"io"
	"encoding/binary"
)

const BlockSize uint16 = 8192

func ParsePageHeader(data []byte) (uint64, bool) {
	res := C.GetLSNIfPageIsValid(unsafe.Pointer(&data[0]))

	// this mess is caused by STDC _Bool
	if res.success != 0 {
		return uint64(res.lsn), true
	}
	return uint64(res.lsn), false
}

func IsPagedFile(info os.FileInfo) bool {
	StaticStructAllignmentCheck()
	name := info.Name()

	if info.IsDir() ||
		strings.HasSuffix(name, "_fsm") ||
		strings.HasSuffix(name, "_vm") ||
		info.Size() == 0 ||
		info.Size()%int64(BlockSize) != 0 {
		return false
	}
	return true
}

func StaticStructAllignmentCheck() {
	var dummy C.PageHeaderData
	sizeof := unsafe.Sizeof(dummy)
	if sizeof != 24 {
		panic("Error in PageHeaderData struct compilation");
	}
}

type IncrementalPageReader struct {
	backlog chan []byte
	file    io.ReadCloser
	info    os.FileInfo
	lsn     uint64
	next    *[]byte
}

func (pr *IncrementalPageReader) Read(p []byte) (n int, err error) {
	var bytesWritten = 0

	return bytesWritten, nil
}

func (pr *IncrementalPageReader) Close() error {
	return pr.file.Close()
}

func (pr *IncrementalPageReader) Initialize() {
	pr.backlog <- []byte{0, 1, 0xAA, 0x55}; //format version 0.1, signature magic number
	fileSizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(fileSizeBytes, uint64(pr.info.Size()))
	pr.backlog <- fileSizeBytes
}

func ReadDatabaseFile(info os.FileInfo, lsn uint64) (io.ReadCloser, bool, error) {
	file, err := os.Open(info.Name())

	if err != nil {
		return nil, false, err
	}

	if !IsPagedFile(info) {
		return file, false, nil
	}

	reader := &IncrementalPageReader{make(chan []byte, 32), file, info, lsn, nil}
	reader.Initialize()
	return reader, true, nil
}

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
	"errors"
	"math"
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
	backlog            chan []byte
	file               io.Reader
	closer             io.Closer
	info               os.FileInfo
	lsn                uint64
	next               *[]byte
	currentBlockNumber int32
	EOFed              bool
}

func (pr *IncrementalPageReader) Read(p []byte) (n int, err error) {
	err = nil
	if pr.next == nil {
		return 0, io.EOF
	}
	n = copy(p, *pr.next)
	if n == len(*pr.next) {
		pr.next = nil
	} else {
		bytes := (*(pr.next))[n:]
		pr.next = &(bytes)
	}

	if pr.next == nil {
		err = pr.DrainMoreData()
	}

	return n, err
}
func (pr *IncrementalPageReader) DrainMoreData() error {
	if !pr.EOFed && len(pr.backlog) < 2 {
		err := pr.AdvanceFileReader()
		if err != nil {
			return err
		}
	}

	if len(pr.backlog) > 0 {
		moreBytes := <-pr.backlog
		pr.next = &moreBytes
	}

	return nil
}

func (pr *IncrementalPageReader) AdvanceFileReader() error {
	pageBytes := make([]byte, BlockSize)
	for {
		n, err := io.ReadFull(pr.file, pageBytes)
		pr.currentBlockNumber++
		if err == io.ErrUnexpectedEOF || n%int(BlockSize) != 0 {
			return errors.New("Unexpected EOF during increment scan")
		}

		if err == io.EOF {
			pr.EOFed = true

			//At the end of a increment we place sentinel to ensure consistency
			blockNumberBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(blockNumberBytes, math.MaxUint32)
			pr.backlog <- blockNumberBytes
			return nil
		}

		if err == nil && n > 0 {
			lsn, valid := ParsePageHeader(pageBytes)
			if (!valid) || (lsn >= pr.lsn) {
				blockNumberBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(blockNumberBytes, uint32(pr.currentBlockNumber))
				pr.backlog <- blockNumberBytes
				pr.backlog <- pageBytes
				break;
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (pr *IncrementalPageReader) Close() error {
	return pr.closer.Close()
}

func (pr *IncrementalPageReader) Initialize() {
	pr.next = &[]byte{0, 1, 1, 0x55}; //format version 0.1, type 1, signature magic number
	fileSizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(fileSizeBytes, uint64(pr.info.Size()))
	pr.backlog <- fileSizeBytes
}

func ReadDatabaseFile(fileName string, lsn *uint64) (io.ReadCloser, bool, error) {
	info, err := os.Stat(fileName)
	if err != nil {
		return nil, false, err
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, false, err
	}

	if lsn==nil || !IsPagedFile(info) {
		return file, false, nil
	}

	lim := &io.LimitedReader{
		R: file,
		N: int64(info.Size()),
	}

	reader := &IncrementalPageReader{make(chan []byte, 32), lim, file, info, *lsn, nil, -1, false}
	reader.Initialize()
	return reader, true, nil
}

func ApplyFileIncrement(fileName string, increment io.Reader) (error) {
	header := make([]byte, 4)
	fileSizeBytes := make([]byte, 8)

	io.ReadFull(increment, header)
	io.ReadFull(increment, fileSizeBytes)

	fileSize := binary.BigEndian.Uint64(fileSizeBytes)

	if header[0] != 0 || header[1] != 1 ||
		header[2] != 1 || header[3] != 0x55 {
		return errors.New("Inconsistent increment header for " + fileName)
	}

	file, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		return err
	}

	err = file.Truncate(int64(fileSize))
	if err != nil {
		return err
	}

	int32Bytes := make([]byte, 4)
	page := make([]byte, BlockSize)
	_, err = io.ReadFull(increment, int32Bytes)
	if err != nil {
		return err
	}
	blockNo := binary.BigEndian.Uint32(int32Bytes)

	for blockNo != math.MaxUint32 {
		_, err = io.ReadFull(increment, page)
		if err != nil {
			return err
		}
		_, err = file.WriteAt(page, int64(blockNo)*int64(BlockSize))
		if err != nil {
			return err
		}

		_, err = io.ReadFull(increment, int32Bytes)
		if err != nil {
			return err
		}
		blockNo = binary.BigEndian.Uint32(int32Bytes)
	}

	n, err := increment.Read(int32Bytes)
	if err != io.EOF || n > 0 {
		return errors.New("Inconsistent sentinel at the end of increment for " + fileName)
	}

	return nil
}

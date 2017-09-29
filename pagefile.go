package walg

/*
#include <inttypes.h>
typedef struct PageHeaderData
{
	uint32_t 		pd_lsn_h;
	uint32_t 		pd_lsn_l;

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

	result.lsn = (((uint64_t)data->pd_lsn_h) << 32) + ((uint64_t)data->pd_lsn_l);

	if ((data->pd_flags & valid_flags) != data->pd_flags ||
		data->pd_lower < header_size ||
		data->pd_lower > data->pd_upper ||
		data->pd_upper > data->pd_special ||
		data->pd_special > block_size ||
		(result.lsn == invalid_lsn)||
		data->pd_pagesize_version != block_size + layout_version)
	{
		return result;
	}

	result.success = 1;
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
	"fmt"
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
	file    *io.LimitedReader
	seeker  io.Seeker
	closer  io.Closer
	info    os.FileInfo
	lsn     uint64
	next    *[]byte
	blocks  []uint32
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
	for len(pr.blocks) > 0 && len(pr.backlog) < 2 {
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
	blockNo := pr.blocks[0]
	pr.blocks = pr.blocks[1:]
	offset := int64(blockNo) * int64(BlockSize)
	_, err := pr.seeker.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(pr.file, pageBytes)
	if err == nil {
		pr.backlog <- pageBytes
	}
	return err
}

func (pr *IncrementalPageReader) Close() error {
	return pr.closer.Close()
}

var InvalidBlock = errors.New("Block is not valid")

func (pr *IncrementalPageReader) Initialize() (size int64, err error) {
	size = 0
	pr.next = &[]byte{0, 1, 1, 0x55}; //format version 0.1, type 1, signature magic number
	size += 4
	fileSizeBytes := make([]byte, 8)
	fileSize := pr.info.Size()
	binary.LittleEndian.PutUint64(fileSizeBytes, uint64(fileSize))
	pr.backlog <- fileSizeBytes
	size += 8

	pageBytes := make([]byte, BlockSize)
	pr.blocks = make([]uint32, 0, fileSize/int64(BlockSize))

	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		n, err := io.ReadFull(pr.file, pageBytes)
		if err == io.ErrUnexpectedEOF || n%int(BlockSize) != 0 {
			return 0, errors.New("Unexpected EOF during increment scan")
		}

		if err == io.EOF {
			diffBlockCount := len(pr.blocks)
			lenBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenBytes, uint32(diffBlockCount))
			pr.backlog <- lenBytes
			size += 4

			diffMap := make([]byte, diffBlockCount*4)

			for index, blockNo := range pr.blocks {
				binary.LittleEndian.PutUint32(diffMap[index*4:index*4+4], blockNo)
			}

			pr.backlog <- diffMap
			size += int64(diffBlockCount * 4)
			dataSize := int64(len(pr.blocks)) * int64(BlockSize)
			size += dataSize
			_, err := pr.seeker.Seek(0, 0)
			if err != nil {
				return 0, nil
			}
			pr.file.N = dataSize
			return size, nil
		}

		if err == nil {
			lsn, valid := ParsePageHeader(pageBytes)
			//fmt.Printf("pr.lsn %x page lsn %x valid %v\n",pr.lsn,lsn,valid)
			var allZeroes = false
			if !valid && allZero(pageBytes) {
				allZeroes = true
				valid = true
			}

			if !valid {
				return 0, InvalidBlock
			}

			if (allZeroes) || (lsn >= pr.lsn) {
				pr.blocks = append(pr.blocks, currentBlockNumber)
			}
		} else {
			return 0, err
		}
	}
}

func ReadDatabaseFile(fileName string, lsn *uint64) (io.ReadCloser, bool, int64, error) {
	info, err := os.Stat(fileName)
	fileSize := info.Size()
	if err != nil {
		return nil, false, fileSize, err
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, false, fileSize, err
	}

	if lsn == nil || !IsPagedFile(info) {
		return file, false, fileSize, nil
	}

	lim := &io.LimitedReader{
		R: io.MultiReader(file, &ZeroReader{}),
		N: int64(fileSize),
	}

	reader := &IncrementalPageReader{make(chan []byte, 32), lim, file, file, info, *lsn, nil, nil}
	incrSize, err := reader.Initialize()
	if err != nil {
		if err == InvalidBlock {
			file.Close()
			fmt.Printf("File %v has invalid pages, fallback to full backup\n")
			file, err = os.Open(fileName)
			if err != nil {
				return nil, false, fileSize, err
			}
			return file, false, fileSize, nil
		} else {
			return nil, false, fileSize, err
		}
	}
	return reader, true, incrSize, nil
}

func ApplyFileIncrement(fileName string, increment io.Reader) (error) {
	fmt.Println("Incrementing " + fileName)
	header := make([]byte, 4)
	fileSizeBytes := make([]byte, 8)
	diffBlockBytes := make([]byte, 4)

	_, err := io.ReadFull(increment, header)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(increment, fileSizeBytes)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(increment, diffBlockBytes)
	if err != nil {
		return err
	}

	fileSize := binary.LittleEndian.Uint64(fileSizeBytes)
	diffBlockCount := binary.LittleEndian.Uint32(diffBlockBytes)
	diffMap := make([]byte, diffBlockCount*4)

	_, err = io.ReadFull(increment, diffMap)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	defer file.Sync()
	defer file.Close()
	if err != nil {
		return err
	}

	err = file.Truncate(int64(fileSize))
	if err != nil {
		return err
	}

	page := make([]byte, BlockSize)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*4:i*4+4])
		_, err = io.ReadFull(increment, page)
		if err != nil {
			return err
		}

		_, err = file.WriteAt(page, int64(blockNo)*int64(BlockSize))
		if err != nil {
			return err
		}

	}

	return nil
}

func allZero(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

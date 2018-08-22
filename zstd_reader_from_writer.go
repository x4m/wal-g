package walg

import (
	"github.com/DataDog/zstd"
	"io"
)

type ZstdReaderFromWriter struct {
	w  zstd.Writer
	pw *io.PipeWriter
}

func NewZstdReaderFromWriter(dst io.Writer) *ZstdReaderFromWriter {
	zstdWriter := zstd.NewWriterLevel(dst, 3)

	//pipeReader, pipeWriter := io.Pipe()
	//
	//go func() {
	//	err := extractOne(&NOPTarInterpreter{"PreZstd"}, pipeReader)
	//	if err != nil {
	//		log.Printf("failed to write tar correctly!!!!")
	//		panic(err)
	//	}
	//	pipeReader.Close()
	//}()
	return &ZstdReaderFromWriter{w: *zstdWriter, pw: nil}
}

func (writer *ZstdReaderFromWriter) Write(p []byte) (n int, err error) {
	//writer.pw.Write(p)
	return writer.w.Write(p)
}

func (writer *ZstdReaderFromWriter) Close() (err error) {
	return writer.w.Close()
}

func (writer *ZstdReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = FastCopy(writer, reader)
	return
}

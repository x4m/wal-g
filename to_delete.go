package walg

import (
	"io"
	"archive/tar"
	"fmt"
	"log"
	"os"
)

// NOPTarInterpreter mocks a tar extractor.
type NOPTarInterpreter struct{}

// Interpret does not do anything except print the
// 'tar member' name.
func (tarInterpreter *NOPTarInterpreter) Interpret(tr io.Reader, header *tar.Header) error {
	fmt.Println(header.Name)
	return nil
}

func NewExtractionCheckingReader(underlying io.Reader) io.Reader {
	pipeReader, pipeWriter := io.Pipe()
	teeReader := io.TeeReader(underlying, pipeWriter)
	go func() {
		err := ExtractAll(&NOPTarInterpreter{}, []ReaderMaker{&UnderlyingReaderMaker{pipeReader}})
		if err != nil {
			log.Printf("failed to write tar correctly!!!!")
			panic(err)
		}
		pipeReader.Close()
	} ()
	return teeReader
}

type UnderlyingReaderMaker struct {
	underlying io.ReadCloser
}

func (readerMaker *UnderlyingReaderMaker) Reader() (io.ReadCloser, error) {
	return readerMaker.underlying, nil
}

func (readerMaker *UnderlyingReaderMaker) Path() string {
	compressionMethod := os.Getenv("WALG_COMPRESSION_METHOD")
	if compressionMethod == "" {
		compressionMethod = Lz4AlgorithmName
	}
	return "some_path." + Compressors[compressionMethod].FileExtension()
}


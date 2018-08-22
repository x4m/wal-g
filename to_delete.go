package walg

import (
	"io"
	"archive/tar"
	"os"
	"fmt"
)

// NOPTarInterpreter mocks a tar extractor.
type NOPTarInterpreter struct {
	prefix string
}

var someSpareBytes = make([]byte, 32768)
var someSpareBytesRead int

// Interpret does not do anything except print the
// 'tar member' name.
func (tarInterpreter *NOPTarInterpreter) Interpret(tr io.Reader, header *tar.Header) error {
	fmt.Println("Unpacked ", header)
	for {
		n, err := tr.Read(someSpareBytes)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		someSpareBytesRead = n
	}
	return nil
}

func NewExtractionCheckingReader(underlying io.Reader) io.Reader {
	pipeReader, pipeWriter := io.Pipe()
	teeReader := io.TeeReader(underlying, pipeWriter)
	go func() {
		err := ExtractAll(&NOPTarInterpreter{}, []ReaderMaker{&UnderlyingReaderMaker{pipeReader}})
		if err != nil {
			fmt.Printf("failed to write tar correctly!!!!")
			fmt.Println(someSpareBytesRead)
			fmt.Println(someSpareBytes)
			panic(err)
		}
		pipeReader.Close()
	}()
	return teeReader
	return underlying
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

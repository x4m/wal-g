package main

import (
	"github.com/wal-g/wal-g"
	"os"
	"fmt"
	"io/ioutil"
	"io"
	"bytes"
	"log"
)

func walkfunc(path string, info os.FileInfo, err error) error {
	fmt.Println(path)
	if !walg.IsPagedFile(info) {
		fmt.Println("Not a paged file")
		return nil
	}

	f, _ := os.Open(path)
	var buf = make([]byte, 8192)

	n, _ := f.Read(buf)
	var i = 0;
	for n > 24 {
		lsn, correct := walg.ParsePageHeader(buf)
		if i < 4 || i%10024 == 0 || !correct {
			fmt.Printf("Page %v lsn %x correct %v \n", i, lsn, correct)
		}
		if !correct {
			fmt.Println(buf[:127])
			fmt.Println("not a correct page, all zeroes are free page")
		}
		i++
		n, _ = f.Read(buf)
	}

	return nil
}

func main() {
	//filepath.Walk("/Users/x4mmm/DemoDb/base/", walkfunc)
	fileName := "testdata/paged_file.bin"
	file, _ := os.Stat(fileName)
	reader, isPaged, err := walg.ReadDatabaseFile(fileName, 0xc6bd460000000000)
	if err != nil {
		fmt.Print(err.Error())
	}
	buf, _ := ioutil.ReadAll(reader)
	fmt.Printf("IsPaged %v\n", isPaged)
	fmt.Printf("Increment size = %v\n", (len(buf)))
	fmt.Printf("Filesize = %v\n", (file.Size()))

	tmpFileName := fileName + "_tmp"
	CopyFile(fileName, tmpFileName)

	tmpFile, _ := os.OpenFile(tmpFileName, os.O_RDWR, 0666)
	tmpFile.WriteAt(make([]byte, 12345), file.Size()-12345)
	tmpFile.Close()

	err = walg.ApplyFileIncrement(tmpFileName, bytes.NewReader(buf))
	if err != nil {
		fmt.Print(err.Error())
	}

	compare := deepCompare(fileName, tmpFileName)
	fmt.Printf("Compare result %v", compare)

	os.Remove(tmpFileName)
}

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

const chunkSize = 64000

func deepCompare(file1, file2 string) bool {
	// Check file size ...

	f1, err := os.Open(file1)
	if err != nil {
		log.Fatal(err)
	}

	f2, err := os.Open(file2)
	if err != nil {
		log.Fatal(err)
	}

	for {
		b1 := make([]byte, chunkSize)
		_, err1 := f1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := f2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return true
			} else if err1 == io.EOF || err2 == io.EOF {
				return false
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			return false
		}
	}
}

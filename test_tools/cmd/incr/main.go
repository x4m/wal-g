package main

import (
	"github.com/wal-g/wal-g"
	"path/filepath"
	"os"
	"fmt"
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
	filepath.Walk("/Users/x4mmm/DemoDb/base/", walkfunc)
}

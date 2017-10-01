package main

import (
	"flag"
	"fmt"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/test_tools"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"time"
)

var profile bool
var mem bool
var nop bool
var s3 bool
var outDir string

func init() {
	flag.BoolVar(&profile, "p", false, "\tProfiler (false by default)")
	flag.BoolVar(&mem, "m", false, "\tMemory profiler (false by default)")
	flag.BoolVar(&nop, "n", false, "\tUse a NOP writer (false by default).")
	flag.BoolVar(&s3, "s", false, "\tUpload compressed tar files to s3 (write to disk by default)")
	flag.StringVar(&outDir, "out", "", "\tDirectory compressed tar files will be written to (unset by default)")
}

func main() {
	flag.Parse()
	all := flag.Args()
	part, err := strconv.Atoi(all[0])
	if err != nil {
		panic(err)
	}
	in := all[1]

	bundle := &walg.Bundle{
		MinSize: int64(part),
	}

	if nop {
		bundle.Tbm = &tools.NOPTarBallMaker{
			BaseDir: filepath.Base(in),
			Trim:    in,
			Nop:     true,
		}
	} else if !s3 && outDir == "" {
		fmt.Printf("Please provide a directory to write to.\n")
		os.Exit(1)
	} else if !s3 {
		if profile {
			f, err := os.Create("cpu.prof")
			if err != nil {
				log.Fatal(err)
			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}

		if mem {
			f, err := os.Create("mem.prof")
			if err != nil {
				log.Fatal(err)
			}

			pprof.WriteHeapProfile(f)
			f.Close()
		}

		bundle.Tbm = &tools.FileTarBallMaker{
			BaseDir: filepath.Base(in),
			Trim:    in,
			Out:     outDir,
		}
		os.MkdirAll(outDir, 0766)

	} else if s3 {
		tu, _, _ := walg.Configure()
		c, err := walg.Connect()
		if err != nil {
			panic(err)
		}

		n, _, err := walg.StartBackup(c, time.Now().String())
		if err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(1)
		}

		bundle.Tbm = &walg.S3TarBallMaker{
			BaseDir:  filepath.Base(in),
			Trim:     in,
			BkupName: n,
			Tu:       tu,
		}

		bundle.NewTarBall()
		bundle.HandleLabelFiles(c)

	}

	bundle.NewTarBall()
	defer tools.TimeTrack(time.Now(), "MAIN")
	fmt.Println("Walking ...")
	err = filepath.Walk(in, bundle.TarWalker)
	if err != nil {
		panic(err)
	}
	err = bundle.Tb.CloseTar()
	if err != nil {
		panic(err)
	}
	err = bundle.Tb.Finish()
	if err != nil {
		panic(err)
	}

}

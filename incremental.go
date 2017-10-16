// This file contains function for handling incremental backup

package walg

import (
	"log"
	"github.com/aws/aws-sdk-go/aws"
	"io/ioutil"
	"encoding/json"
	"os"
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"time"
	"runtime/pprof"
)

func HandleIncrementalFetch(backupName string, pre *Prefix, dirArc string, mem bool) (lsn uint64) {
	lsn = IncrementalFetchRecursion(backupName, pre, dirArc)

	if mem {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(f)
		defer f.Close()
	}
	return
}

// This function composes Backup object and recursively searches for necessary base backup
func IncrementalFetchRecursion(backupName string, pre *Prefix, dirArc string) uint64 {
	var bk *Backup
	// Check if BACKUPNAME exists and if it does extract to DIRARC.
	if backupName != "LATEST" {
		bk = &Backup{
			Prefix: pre,
			Path:   aws.String(*pre.Server + "/basebackups_005/"),
			Name:   aws.String(backupName),
		}
		bk.Js = aws.String(*bk.Path + *bk.Name + "_backup_stop_sentinel.json")

		exists, err := bk.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		if !exists {
			log.Fatalf("Backup '%s' does not exist.\n", *bk.Name)
		}

		// Find the LATEST valid backup (checks against JSON file and grabs backup name) and extract to DIRARC.
	} else {
		bk = &Backup{
			Prefix: pre,
			Path:   aws.String(*pre.Server + "/basebackups_005/"),
		}

		latest, err := bk.GetLatest()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		bk.Name = aws.String(latest)
	}
	var dto = fetchSentinel(*bk.Name, bk, pre)

	if dto.IsIncremental() {
		fmt.Printf("Increment from %v at LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN)
		IncrementalFetchRecursion(*dto.IncrementFrom, pre, dirArc)
		fmt.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN, dto.LSN)
	}

	UnwrapBackup(bk, dirArc, pre, dto)
	if dto.LSN != nil {
		return *dto.LSN
	}
	return 0
}

// Do the job of unpacking Backup object
func UnwrapBackup(bk *Backup, dirArc string, pre *Prefix, sentinel S3TarBallSentinelDto) {

	incrementBase := path.Join(dirArc, "increment_base")
	if !sentinel.IsIncremental() {
		var empty = true
		searchLambda := func(path string, info os.FileInfo, err error) error {
			if path != dirArc {
				empty = false
			}
			return nil
		}
		filepath.Walk(dirArc, searchLambda)

		if !empty {
			log.Fatalf("Directory %v for increment base must be empty", dirArc)
		}
	} else {
		defer func() {
			err := os.RemoveAll(incrementBase)
			if err != nil {
				log.Fatal(err)
			}
		}()

		err := os.MkdirAll(incrementBase, os.FileMode(0777))
		if err != nil {
			log.Fatal(err)
		}

		files, err := ioutil.ReadDir(dirArc)
		if err != nil {
			log.Fatal(err)
		}

		for _, f := range files {
			objName := f.Name()
			if objName != "increment_base" {
				err := os.Rename(path.Join(dirArc, objName), path.Join(incrementBase, objName))
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		for fileName, fd := range sentinel.Files {
			if !fd.IsSkipped {
				continue
			}
			fmt.Printf("Skipped file %v\n", fileName)
			targetPath := path.Join(dirArc, fileName)
			// this path is only used for increment restoration
			incrementalPath := path.Join(incrementBase, fileName)
			err = MoveFileAndCreateDirs(incrementalPath, targetPath, fileName)
			if err != nil {
				log.Fatal(err, "Failed to move skipped file for "+targetPath+" "+fileName)
			}
		}

	}

	var allKeys []string
	var keys []string
	allKeys, err := bk.GetKeys()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	keys = allKeys[:len(allKeys)-1]
	f := &FileTarInterpreter{
		NewDir:             dirArc,
		Sentinel:           sentinel,
		IncrementalBaseDir: incrementBase,
	}
	out := make([]ReaderMaker, len(keys))
	for i, key := range keys {
		s := &S3ReaderMaker{
			Backup:     bk,
			Key:        aws.String(key),
			FileFormat: CheckType(key),
		}
		out[i] = s
	}
	// Extract all compressed tar members except `pg_control.tar.lz4` if WALG version backup.
	err = ExtractAll(f, out)
	if serr, ok := err.(*UnsupportedFileTypeError); ok {
		log.Fatalf("%v\n", serr)
	} else if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	match := re.FindString(*bk.Name)
	if match == "" {
		// Extract pg_control last. If pg_control does not exist, program exits with error code 1.
		name := *bk.Path + *bk.Name + "/tar_partitions/pg_control.tar.lz4"
		pgControl := &Archive{
			Prefix:  pre,
			Archive: aws.String(name),
		}

		exists, err := pgControl.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		if exists {
			sentinel := make([]ReaderMaker, 1)
			sentinel[0] = &S3ReaderMaker{
				Backup:     bk,
				Key:        aws.String(name),
				FileFormat: CheckType(name),
			}
			err := ExtractAll(f, sentinel)
			if serr, ok := err.(*UnsupportedFileTypeError); ok {
				log.Fatalf("%v\n", serr)
			} else if err != nil {
				log.Fatalf("%+v\n", err)
			}
			fmt.Printf("\nBackup extraction complete.\n")
		} else {
			log.Fatal("Corrupt backup: missing pg_control")
		}
	}
}

func HandleIncrementalBackup(dirArc string, tu *TarUploader, pre *Prefix) {
	var bk = &Backup{
		Prefix: pre,
		Path:   aws.String(*pre.Server + "/basebackups_005/"),
	}

	var dto S3TarBallSentinelDto
	latest, err := bk.GetLatest()
	if err != LatestNotFound {
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		dto = fetchSentinel(latest, bk, pre)
	}

	if dto.LSN == nil {
		fmt.Println("LATEST backup was made without increment feature. Fallback to full backup with increment LSN marker.")
	} else {
		fmt.Printf("Incremental backup from %v with LSN %x. \n", latest, *dto.LSN)
	}

	// Connect to postgres and start/finish a nonexclusive backup.
	bundle := &Bundle{
		MinSize:            int64(1000000000), //MINSIZE = 1GB
		IncrementFromLsn:   dto.LSN,
		IncrementFromFiles: dto.Files,
	}
	if dto.Files == nil {
		bundle.IncrementFromFiles = make(map[string]BackupFileDescription)
	}
	conn, err := Connect()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	n, lsn, err := StartBackup(conn, time.Now().String())
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Start a new tar bundle and walk the DIRARC directory and upload to S3.
	bundle.Tbm = &S3TarBallMaker{
		BaseDir:          filepath.Base(dirArc),
		Trim:             dirArc,
		BkupName:         n,
		Tu:               tu,
		Lsn:              &lsn,
		IncrementFromLsn: dto.LSN,
		IncrementFrom:    latest,
	}
	bundle.NewTarBall()
	fmt.Println("Walking ...")
	err = filepath.Walk(dirArc, bundle.TarWalker)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	err = bundle.Tb.CloseTar()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Upload `pg_control`.
	err = bundle.HandleSentinel()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` files
	err = bundle.HandleLabelFiles(conn)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Wait for all uploads to finish.
	err = bundle.Tb.Finish()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func fetchSentinel(backupName string, bk *Backup, pre *Prefix) (dto S3TarBallSentinelDto) {
	latestSentinel := backupName + "_backup_stop_sentinel.json"
	previousBackupReader := S3ReaderMaker{
		Backup:     bk,
		Key:        aws.String(*pre.Server + "/basebackups_005/" + latestSentinel),
		FileFormat: CheckType(latestSentinel),
	}
	prevBackup, err := previousBackupReader.Reader()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	sentinelDto, err := ioutil.ReadAll(prevBackup)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	err = json.Unmarshal(sentinelDto, &dto)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	return
}

package main

import (
	"github.com/wal-g/wal-g"
	"log"
	"os"
	"os/exec"
	"time"
)

func main() {
	baseDir := "/Users/x4mmm/DemoDb"
	restoreDir := "/Users/x4mmm/DemoDbRestore"
	pgbenchCommand:= "/Users/x4mmm/project/bin/pgbench postgres"
	os.Setenv("WALE_S3_PREFIX", os.Getenv("WALE_S3_PREFIX")+"/"+time.Now().String())
	tu, pre, err := walg.Configure()
	if err != nil {
		log.Fatal(err);
	}

	os.Remove(restoreDir)
	os.MkdirAll(restoreDir,0777)

	walg.HandleIncrementalBackup(baseDir, tu, pre);
	exec.Command(pgbenchCommand)

}

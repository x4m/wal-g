package internal

import (
	"bytes"
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/x4m/wal-g/internal/tracelog"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func HandleStreamFetch(backupName string, folder StorageFolder) {
	if backupName == "" || backupName == "LATEST" {
		latest, err := getLatestBackupName(folder)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("Unable to get latest backup %+v\n", err)
		}
		backupName = latest
	}
	stat, _ := os.Stdout.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
	} else {
		tracelog.ErrorLogger.Fatalf("stout is a terminal")
	}
	err := downloadAndDecompressStream(folder, backupName)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
func downloadAndDecompressStream(folder StorageFolder, fileName string) error {
	baseBackupFolder := folder.GetSubFolder(BaseBackupPath)
	backup := NewBackup(baseBackupFolder, fileName)

	streamSentinel, err := backup.fetchStreamSentinel()
	if err != nil {
		return err
	}
	binlogsAreDone := make(chan error)

	go fetchBinlogs(folder, streamSentinel, binlogsAreDone)

	for _, decompressor := range Decompressors {
		d := decompressor
		archiveReader, exists, err := TryDownloadWALFile(baseBackupFolder, getStreamName(backup, d.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		err = decompressWALFile(&EmptyWriteIgnorer{os.Stdout}, archiveReader, d)
		os.Stdout.Close()

		tracelog.DebugLogger.Println("Waiting for binlogs")
		err = <-binlogsAreDone

		return err
	}
	return ArchiveNonExistenceError{errors.Errorf("Archive '%s' does not exist.\n", fileName)}
}

func fetchBinlogs(folder StorageFolder, sentinel StreamSentinelDto, binlogsAreDone chan error) {
	binlogFolder := folder.GetSubFolder(BinlogPath)
	endTS, dstFolder := GetBinlogConfigs()
	if dstFolder == "" {
		binlogsAreDone <- errors.New("WALG_MYSQL_BINLOG_DST is not configured")
		return
	}
	objects, _, err := binlogFolder.ListFolder()
	if err != nil {
		binlogsAreDone <- nil
		return
	}

	for _, object := range objects {
		tracelog.DebugLogger.Println("Consider binlog ", object.GetName(), object.GetLastModified().Format(time.RFC3339))

		binlogName := ExtractBinlogName(object, folder)

		if (BinlogShouldBeFetched(sentinel, binlogName, endTS, object)) {
			fileName := path.Join(dstFolder, binlogName)
			tracelog.DebugLogger.Println("Download", binlogName, "to", fileName)
			err := downloadWALFileTo(binlogFolder, binlogName, fileName)
			if err != nil {
				binlogsAreDone <- err
				return
			}
		}
	}

	binlogsAreDone <- nil
}

func checkOldBinlogs(backupTime BackupTime, folder StorageFolder) {
	if !strings.HasPrefix(backupTime.BackupName, StreamPrefix) {
		return
	}
	backup := NewBackup(folder.GetSubFolder(BaseBackupPath), backupTime.BackupName)
	dto, err := backup.fetchStreamSentinel()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	binlogSkip := dto.BinLogStart
	tracelog.InfoLogger.Println("Delete binlog before", binlogSkip)
	deleteWALBefore(binlogSkip, folder.GetSubFolder(BinlogPath))
}

func GetBinlogConfigs() (*time.Time, string) {
	endTSStr := getSettingValue("WALG_MYSQL_BINLOG_END_TS")
	var endTS *time.Time
	if endTSStr != "" {
		if t, err := time.Parse(time.RFC3339, endTSStr); err == nil {
			endTS = &t
		}
	}
	dstFolder := getSettingValue("WALG_MYSQL_BINLOG_DST")
	return endTS, dstFolder
}

func BinlogShouldBeFetched(sentinel StreamSentinelDto, binlogName string, endTS *time.Time, object StorageObject) bool {
	return sentinel.BinLogStart <= binlogName && (endTS == nil || (*endTS).After(object.GetLastModified()))
}

func ExtractBinlogName(object StorageObject, folder StorageFolder) string {
	binlogName := object.GetName()
	return strings.TrimSuffix(binlogName, "."+GetFileExtension(binlogName))
}

func HandleStreamPush(uploader *Uploader, backupName string) {
	uploader.uploadingFolder = uploader.uploadingFolder.GetSubFolder(BaseBackupPath)
	db, err := getMySQLConnection()
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}

	if backupName == "" {
		backupName = StreamPrefix + time.Now().UTC().Format("20060102T150405Z")
	}
	stat, _ := os.Stdin.Stat()
	var stream io.Reader = os.Stdin
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		tracelog.InfoLogger.Println("Data is piped from stdin")
		//tracelog.ErrorLogger.Println("WARNING: operating in test mode!")
		//stream = strings.NewReader("testtesttest")
	} else {
		//tracelog.ErrorLogger.Fatalf("stdin is from a terminal")
		tracelog.ErrorLogger.Println("WARNING: stdin is terminal: operating in test mode!")
		stream = strings.NewReader("testtesttest")
	}
	err = uploader.UploadStream(backupName, db, stream)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

const (
	BinlogPath               = "binlog_" + VersionStr + "/"
	MysqlBinlogCacheFileName = ".walg_mysql_logs_cache"
	StreamPrefix             = "stream_"
)

func HandleMySQLCron(uploader *Uploader, backupLocation string) {
	uploader.uploadingFolder = uploader.uploadingFolder.GetSubFolder(BinlogPath)
	db, err := getMySQLConnection()
	defer db.Close()

	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}

	binlogs := getMySQLSortedBinlogs(db)

	for _, log := range binlogs {
		err = tryArchiveBinLog(uploader, path.Join(backupLocation, log), log)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("%+v\n", err)
		}
	}
}

func tryArchiveBinLog(uploader *Uploader, filename string, log string) error {
	if log <= getLastArchivedBinlog() {
		tracelog.InfoLogger.Printf("Binlog %v already archived\n", log)
		return nil
	}
	tracelog.InfoLogger.Printf("Archiving %v\n", log)

	walFile, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", filename)
	}
	err = uploader.UploadWalFile(walFile)
	if err != nil {
		return errors.Wrapf(err, "upload: could not upload '%s'\n", filename)
	}

	setLastArchivedBinlog(log)
	return nil
}

type MySQLLogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
}

func getLastArchivedBinlog() string {
	var cache MySQLLogsCache
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = filepath.Join(usr.HomeDir, MysqlBinlogCacheFileName)

		var file []byte
		file, err = ioutil.ReadFile(cacheFilename)
		// here we ignore whatever error can occur
		if err == nil {
			err = json.Unmarshal(file, &cache)
			if err == nil {
				return cache.LastArchivedBinlog
			}
		}
	}
	if os.IsNotExist(err) {
		tracelog.InfoLogger.Println("MySQL cache does not exist")
	} else {
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}
	return ""
}

func setLastArchivedBinlog(binlogFileName string) {
	var cache MySQLLogsCache
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = filepath.Join(usr.HomeDir, MysqlBinlogCacheFileName)
		var file []byte
		file, err = ioutil.ReadFile(cacheFilename)
		// here we ignore whatever error can occur
		if err == nil {
			_ = json.Unmarshal(file, &cache)
		}
	}
	if err != nil && !os.IsNotExist(err) {
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}

	cache.LastArchivedBinlog = binlogFileName

	marshal, err := json.Marshal(&cache)
	if err == nil && len(cacheFilename) > 0 {
		_ = ioutil.WriteFile(cacheFilename, marshal, 0644)
	}
}

func getMySQLSortedBinlogs(db *sql.DB) []string {
	var result []string

	currentBinlog := getMySQLCurrentBinlogFile(db)

	rows, err := db.Query("show binary logs")
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
	for rows.Next() {
		var logFinName string
		var size uint32
		err = rows.Scan(&logFinName, &size)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("%+v\n", err)
		}
		if logFinName < currentBinlog {
			result = append(result, logFinName)
		}
	}
	sort.Strings(result)
	return result
}

// TODO : unit tests
func (backup *Backup) fetchStreamSentinel() (StreamSentinelDto, error) {
	sentinelDto := StreamSentinelDto{}
	backupReaderMaker := NewStorageReaderMaker(backup.BaseBackupFolder, backup.getStopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return sentinelDto, err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to fetch sentinel")
	}

	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
}

func getMySQLCurrentBinlogFile(db *sql.DB) (fileName string) {
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
	var logFileName string
	var garbage interface{}
	for rows.Next() {
		err = rows.Scan(&logFileName, &garbage, &garbage, &garbage, &garbage)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("%+v\n", err)
		}
		return logFileName
	}
	rows, err = db.Query("SHOW SLAVE STATUS")
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
	for rows.Next() {
		err = rows.Scan(&logFileName, &garbage, &garbage, &garbage, &garbage)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("%+v\n", err)
		}
		return logFileName
	}
	tracelog.ErrorLogger.Fatalf("Failed to obtain current binlog file")
	return ""
}

func getMySQLConnection() (*sql.DB, error) {
	datasourceName := getSettingValue("WALG_MYSQL_DATASOURCE_NAME")
	if datasourceName == "" {
		datasourceName = "root:password@/mysql"
	}
	db, err := sql.Open("mysql", datasourceName)
	return db, err
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *Uploader) UploadStream(fileName string, db *sql.DB, stream io.Reader) error {

	binlogStart := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog start file", binlogStart)
	timeStart := time.Now()

	compressor := uploader.compressor
	pipeWriter := &CompressingPipeWriter{
		Input:                stream,
		NewCompressingWriter: compressor.NewWriter,
	}

	pipeWriter.Compress(&OpenPGPCrypter{})

	backup := NewBackup(uploader.uploadingFolder, fileName)

	dstPath := getStreamName(backup, compressor.FileExtension())
	tracelog.DebugLogger.Println("Upload path", dstPath);
	reader := pipeWriter.Output

	err := uploader.upload(dstPath, reader)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)
	binlogEnd := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog end file", binlogEnd)

	uploadStreamSentinel(&StreamSentinelDto{BinLogStart: binlogStart, BinLogEnd: binlogEnd, StartLocalTime: timeStart}, uploader, fileName+SentinelSuffix)

	return err
}

type StreamSentinelDto struct {
	BinLogStart    string `json:"BinLogStart,omitempty"`
	BinLogEnd      string `json:"BinLogEnd,omitempty"`
	StartLocalTime time.Time
}

func uploadStreamSentinel(sentinelDto *StreamSentinelDto, uploader *Uploader, name string) error {
	dtoBody, err := json.Marshal(*sentinelDto)
	if err != nil {
		return err
	}

	uploadingErr := uploader.upload(name, bytes.NewReader(dtoBody))
	if uploadingErr != nil {
		tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", name)
		tracelog.ErrorLogger.Fatalf("StorageTarBall finish: json failed to upload")
		return uploadingErr
	}
	return nil
}

func getStreamName(backup *Backup, extension string) string {
	dstPath := sanitizePath(backup.Name+"/stream.") + extension
	return dstPath
}

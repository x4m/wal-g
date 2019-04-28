package internal

import (
	"bytes"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgproto3"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	PgControlPath = "/global/pg_control"
	LatestString  = "LATEST"
)

var UnwrapAll map[string]bool = nil

var UtilityFilePaths = map[string]bool{
	PgControlPath:         true,
	BackupLabelFilename:   true,
	TablespaceMapFilename: true,
}

type BackupNonExistenceError struct {
	error
}

func NewBackupNonExistenceError(backupName string) BackupNonExistenceError {
	return BackupNonExistenceError{errors.Errorf("Backup '%s' does not exist.", backupName)}
}

func (err BackupNonExistenceError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type NonEmptyDbDataDirectoryError struct {
	error
}

func NewNonEmptyDbDataDirectoryError(dbDataDirectory string) NonEmptyDbDataDirectoryError {
	return NonEmptyDbDataDirectoryError{errors.Errorf("Directory %v for delta base must be empty", dbDataDirectory)}
}

func (err NonEmptyDbDataDirectoryError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type PgControlNotFoundError struct {
	error
}

func NewPgControlNotFoundError() PgControlNotFoundError {
	return PgControlNotFoundError{errors.Errorf("Expect pg_control archive, but not found")}
}

func (err PgControlNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(folder storage.Folder, dbDataDirectory string, backupName string) {
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s, folder, %s)\n", backupName, dbDataDirectory)
	dbDataDirectory = ResolveSymlink(dbDataDirectory)
	err := deltaFetchRecursion(backupName, folder, dbDataDirectory, nil)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to fetch backup: %v\n", err)
	}
}

func GetBackupByName(backupName string, folder storage.Folder) (*Backup, error) {
	baseBackupFolder := folder.GetSubFolder(BaseBackupPath)

	var backup *Backup
	if backupName == LatestString {
		latest, err := GetLatestBackupName(folder)
		if err != nil {
			return nil, err
		}
		tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest)

		backup = NewBackup(baseBackupFolder, latest)

	} else {
		backup = NewBackup(baseBackupFolder, backupName)

		exists, err := backup.CheckExistence()
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, NewBackupNonExistenceError(backupName)
		}
	}
	return backup, nil
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursion(backupName string, folder storage.Folder, dbDataDirectory string, filesToUnwrap map[string]bool) error {
	backup, err := GetBackupByName(backupName, folder)
	if err != nil {
		return err
	}
	sentinelDto, err := backup.FetchSentinel()
	if err != nil {
		return err
	}

	if filesToUnwrap == nil { // it is the exact backup we want to fetch, so we want to include all files here
		filesToUnwrap = GetRestoredBackupFilesToUnwrap(sentinelDto)
	}

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN))
		baseFilesToUnwrap, err := GetBaseFilesToUnwrap(sentinelDto.Files, filesToUnwrap)
		if err != nil {
			return err
		}
		err = deltaFetchRecursion(*sentinelDto.IncrementFrom, folder, dbDataDirectory, baseFilesToUnwrap)
		if err != nil {
			return err
		}
		tracelog.InfoLogger.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN), *(sentinelDto.BackupStartLSN))
	}

	return backup.unwrap(dbDataDirectory, sentinelDto, filesToUnwrap)
}

func GetRestoredBackupFilesToUnwrap(sentinelDto BackupSentinelDto) map[string]bool {
	if sentinelDto.Files == nil { // in case of WAL-E of old WAL-G backup
		return UnwrapAll
	}
	filesToUnwrap := make(map[string]bool)
	for file := range sentinelDto.Files {
		filesToUnwrap[file] = true
	}
	for utilityFilePath := range UtilityFilePaths {
		filesToUnwrap[utilityFilePath] = true
	}
	return filesToUnwrap
}

func GetBaseFilesToUnwrap(backupFileStates BackupFileList, currentFilesToUnwrap map[string]bool) (map[string]bool, error) {
	baseFilesToUnwrap := make(map[string]bool)
	for file := range currentFilesToUnwrap {
		fileDescription, hasDescription := backupFileStates[file]
		if !hasDescription {
			if _, ok := UtilityFilePaths[file]; !ok {
				tracelog.ErrorLogger.Panicf("Wanted to fetch increment for file: '%s', but didn't find one in base", file)
			}
			continue
		}
		if fileDescription.IsSkipped || fileDescription.IsIncremented {
			baseFilesToUnwrap[file] = true
		}
	}
	return baseFilesToUnwrap, nil
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupServe(folder storage.Folder, dbDataDirectory string, backupName string) {
	tracelog.DebugLogger.Printf("HandleBackupServe(%s, folder, %s)\n", backupName, dbDataDirectory)

	backup, err := GetBackupByName(backupName, folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	sentinelDto, err := backup.FetchSentinel()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	if sentinelDto.IsIncremental() {
		tracelog.ErrorLogger.Fatal("Cannot serve incremental backup")
	}

	listener, err := net.Listen("tcp", "localhost:5433")
	fatal(err)

	defer listener.Close()
	conn, err := listener.Accept()
	fatal(err)

	backend, err := pgproto3.NewBackend(conn, conn)
	fatal(err)

	message, err := backend.ReceiveStartupMessage()
	fatal(err)

	tracelog.InfoLogger.Println(message)

	backend.Send(&pgproto3.Authentication{Type: pgproto3.AuthTypeOk})
	backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "12devel"})
	backend.Send(&pgproto3.ReadyForQuery{})

	for {
		msg, err := backend.Receive()
		fatal(err)

		tracelog.InfoLogger.Println(reflect.TypeOf(msg))
		tracelog.InfoLogger.Println(msg)
		switch msg.(type) {
		case *pgproto3.Query:
			answerQuery(backend, msg.(*pgproto3.Query), sentinelDto, backup)
		}
	}
}

func fatal(err error) {
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
}

func answerQuery(backend *pgproto3.Backend, query *pgproto3.Query, sentinel BackupSentinelDto, backup *Backup) {
	if (strings.HasPrefix(query.String, "IDENTIFY_SYSTEM")) {
		err := backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{
				{Name: "systemid"},
				{Name: "timeline"},
				{Name: "xlogpos"},
				{Name: "dbname"},
			},
		})
		fatal(err)

		timelineId, _, err := ParseWALFilename(stripWalFileName(backup.Name))
		fatal(err)

		err = backend.Send(&pgproto3.DataRow{Values: [][]byte{
			[]byte("1"),
			[]byte(strconv.Itoa(int(timelineId))),
			[]byte(pgx.FormatLSN(*sentinel.BackupFinishLSN)),
			[]byte(""),
		}})
		fatal(err)

		err = backend.Send(&pgproto3.CommandComplete{CommandTag: "SELECT"})
		fatal(err)
	} else if (strings.HasPrefix(query.String, "SHOW data_directory_mode")) {
		err := backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{{Name: "data_directory_mode"},
			},
		})
		fatal(err)

		err = backend.Send(&pgproto3.DataRow{Values: [][]byte{
			[]byte("0700"),
		}})
		fatal(err)

		err = backend.Send(&pgproto3.CommandComplete{CommandTag: "SELECT"})
		fatal(err)
	} else if (strings.HasPrefix(query.String, "SHOW wal_segment_size")) {
		err := backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{{Name: "wal_segment_size"},
			},
		})
		fatal(err)

		err = backend.Send(&pgproto3.DataRow{Values: [][]byte{
			[]byte("16MB"),
		}})
		fatal(err)

		err = backend.Send(&pgproto3.CommandComplete{CommandTag: "SELECT"})
		fatal(err)
	} else if (strings.HasPrefix(query.String, "BASE_BACKUP")) {
		sendBackup(backend, sentinel, backup)
	}

	err := backend.Send(&pgproto3.ReadyForQuery{})
	fatal(err)
}

func sendBackup(backend *pgproto3.Backend, sentinel BackupSentinelDto, backup *Backup) {
	err := backend.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{Name: "recptr"},
			{Name: "tli"},
		},
	})
	fatal(err)

	timelineId, _, err := ParseWALFilename(stripWalFileName(backup.Name))
	fatal(err)

	err = backend.Send(&pgproto3.DataRow{Values: [][]byte{
		[]byte(pgx.FormatLSN(*sentinel.BackupStartLSN)),
		[]byte(strconv.Itoa(int(timelineId))),
	}})
	fatal(err)

	err = backend.Send(&pgproto3.CommandComplete{CommandTag: "SELECT"})
	fatal(err)

	err = backend.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{Name: "spcoid"},
			{Name: "spclocation"},
			{Name: "size"},
		},
	})
	fatal(err)

	tarsToExtract, _, err := backup.getTarsToExtract()
	fatal(err)
	for range tarsToExtract {
		err = backend.Send(&pgproto3.DataRow{Values: [][]byte{
			nil,
			nil,
			nil,
		}})
	}
	fatal(err)

	err = backend.Send(&pgproto3.CommandComplete{CommandTag: "SELECT"})

	crypter := &OpenPGPCrypter{}
	for _, rm := range tarsToExtract {
		tracelog.InfoLogger.Println("Downloading ", rm.Path())
		buffer := bytes.Buffer{}
		err = DecryptAndDecompressTar(&buffer, rm, crypter)
		fatal(err)
		data := buffer.Bytes()
		tracelog.InfoLogger.Println("sending ", len(data))
		err = backend.Send(&pgproto3.CopyOutResponse{})
		err = backend.Send(&pgproto3.CopyData{Data: data})
		fatal(err)
	}

	err = backend.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{Name: "recptr"},
			{Name: "tli"},
		},
	})
	fatal(err)
	err = backend.Send(&pgproto3.DataRow{Values: [][]byte{
		[]byte(pgx.FormatLSN(*sentinel.BackupFinishLSN)),
		[]byte(strconv.Itoa(int(timelineId))),
	}})
	err = backend.Send(&pgproto3.CommandComplete{CommandTag: "SELECT"})
	fatal(err)
}

func HandleProxy() {
	tracelog.DebugLogger.Printf("AMA Proxy\n")

	listener, err := net.Listen("tcp", "localhost:5433")
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	defer listener.Close()
	conn, err := listener.Accept()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	backend, err := pgproto3.NewBackend(conn, conn)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	dial, err := net.Dial("tcp", "localhost:5432")
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	frontend, err := pgproto3.NewFrontend(dial, dial)

	err = frontend.Send(startupMsg)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	var termination = make(chan interface{})
	inTermination:=false

	go func() {
		for {
			msg, e := frontend.Receive()
			if e != nil {
				tracelog.ErrorLogger.FatalError(e)
			}
			tracelog.InfoLogger.Println("From frontend ", reflect.TypeOf(msg))

			if _, ok := msg.(*pgproto3.CopyData); ok {
				tracelog.InfoLogger.Println("Starting copy")
			} else {
				time.Sleep(time.Millisecond * 150)
				tracelog.InfoLogger.Println(msg.Encode(nil))
			}

			err := backend.Send(msg)
			fatal(err)
		}
	}()

	go func() {
		for {
			msg, e := backend.Receive()
			if e != nil {
				tracelog.ErrorLogger.FatalError(e)
			}
			tracelog.InfoLogger.Println("From backend ", reflect.TypeOf(msg))
			//tracelog.InfoLogger.Println(msg)
			if _, ok := msg.(*pgproto3.Terminate); ok {
				if !inTermination {
					inTermination = true;
					termination <- msg
				}
			}
			err = frontend.Send(msg)
			fatal(err)
		}
	}()

	<-termination
}

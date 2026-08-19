package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// startBaseBackup is compatible with both PostgreSQL's three-column
// tablespace result and PolarDB's four-column result. PolarDB appends a
// shared_storage column and emits data.tar for its shared data directory.
func startBaseBackup(ctx context.Context, conn *pgconn.PgConn, options pglogrepl.BaseBackupOptions) (
	result pglogrepl.BaseBackupResult, err error,
) {
	serverVersion, err := baseBackupServerMajorVersion(conn)
	if err != nil {
		return result, err
	}

	conn.Frontend().SendQuery(&pgproto3.Query{String: baseBackupSQL(options, serverVersion)})
	if err := conn.Frontend().Flush(); err != nil {
		return result, fmt.Errorf("failed to send BASE_BACKUP: %w", err)
	}

	result.LSN, result.TimelineID, err = receiveBaseBackupPosition(ctx, conn)
	if err != nil {
		return result, err
	}
	result.Tablespaces, err = receiveBaseBackupTablespaces(ctx, conn)
	return result, err
}

func baseBackupServerMajorVersion(conn *pgconn.PgConn) (int, error) {
	version := conn.ParameterStatus("server_version")
	major := version
	if dot := strings.IndexByte(version, '.'); dot >= 0 {
		major = version[:dot]
	}
	value, err := strconv.Atoi(major)
	if err != nil {
		return 0, fmt.Errorf("bad server version string %q", version)
	}
	return value, nil
}

func baseBackupSQL(options pglogrepl.BaseBackupOptions, serverVersion int) string {
	parts := make([]string, 0, 5)
	if options.Label != "" {
		parts = append(parts, "LABEL '"+strings.ReplaceAll(options.Label, "'", "''")+"'")
	}
	if options.Progress {
		parts = append(parts, "PROGRESS")
	}
	if options.Fast {
		if serverVersion >= 15 {
			parts = append(parts, "CHECKPOINT 'fast'")
		} else {
			parts = append(parts, "FAST")
		}
	}
	if options.WAL {
		parts = append(parts, "WAL")
	}
	if options.NoWait {
		if serverVersion >= 15 {
			parts = append(parts, "WAIT false")
		} else {
			parts = append(parts, "NOWAIT")
		}
	}
	if options.MaxRate >= 32 {
		parts = append(parts, fmt.Sprintf("MAX_RATE %d", options.MaxRate))
	}
	if options.TablespaceMap {
		parts = append(parts, "TABLESPACE_MAP")
	}
	if options.NoVerifyChecksums {
		if serverVersion >= 15 {
			parts = append(parts, "VERIFY_CHECKSUMS false")
		} else if serverVersion >= 11 {
			parts = append(parts, "NOVERIFY_CHECKSUMS")
		}
	}
	if options.Manifest {
		parts = append(parts, "MANIFEST 'yes'")
		if options.ManifestChecksums != "" {
			parts = append(parts, "MANIFEST_CHECKSUMS '"+
				strings.ReplaceAll(options.ManifestChecksums, "'", "''")+"'")
		}
	}
	if serverVersion >= 17 && options.Incremental {
		parts = append(parts, "INCREMENTAL")
	}
	if serverVersion >= 15 {
		return "BASE_BACKUP(" + strings.Join(parts, ", ") + ")"
	}
	return "BASE_BACKUP " + strings.Join(parts, " ")
}

func receiveBaseBackupPosition(ctx context.Context, conn *pgconn.PgConn) (
	lsn pglogrepl.LSN, timeline int32, err error,
) {
	for {
		msg, recvErr := conn.ReceiveMessage(ctx)
		if recvErr != nil {
			return 0, 0, fmt.Errorf("receive BASE_BACKUP position: %w", recvErr)
		}
		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			if len(msg.Fields) != 2 {
				return 0, 0, fmt.Errorf("BASE_BACKUP position: expected 2 columns, got %d", len(msg.Fields))
			}
		case *pgproto3.DataRow:
			if len(msg.Values) != 2 {
				return 0, 0, fmt.Errorf("BASE_BACKUP position: expected 2 values, got %d", len(msg.Values))
			}
			lsn, err = pglogrepl.ParseLSN(string(msg.Values[0]))
			if err != nil {
				return 0, 0, fmt.Errorf("parse BASE_BACKUP LSN: %w", err)
			}
			value, parseErr := strconv.ParseInt(string(msg.Values[1]), 10, 32)
			if parseErr != nil {
				return 0, 0, fmt.Errorf("parse BASE_BACKUP timeline: %w", parseErr)
			}
			timeline = int32(value)
		case *pgproto3.CommandComplete:
			return lsn, timeline, nil
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
		case *pgproto3.ErrorResponse:
			return 0, 0, pgconn.ErrorResponseToPgError(msg)
		default:
			return 0, 0, fmt.Errorf("BASE_BACKUP position: unexpected response %T", msg)
		}
	}
}

func receiveBaseBackupTablespaces(ctx context.Context, conn *pgconn.PgConn) (
	tablespaces []pglogrepl.BaseBackupTablespace, err error,
) {
	columnCount := 0
	for {
		msg, recvErr := conn.ReceiveMessage(ctx)
		if recvErr != nil {
			return nil, fmt.Errorf("receive BASE_BACKUP tablespaces: %w", recvErr)
		}
		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			columnCount = len(msg.Fields)
			if columnCount != 3 && columnCount != 4 {
				return nil, fmt.Errorf("BASE_BACKUP tablespaces: expected 3 or 4 columns, got %d", columnCount)
			}
		case *pgproto3.DataRow:
			if len(msg.Values) != columnCount || (columnCount != 3 && columnCount != 4) {
				return nil, fmt.Errorf("BASE_BACKUP tablespaces: unexpected value count %d", len(msg.Values))
			}
			// The rows for the main data directory and PolarDB shared data
			// directory have a NULL OID. Their archives are identified by the
			// following CopyData archive headers instead.
			if msg.Values[0] == nil {
				continue
			}
			oid, parseErr := strconv.ParseInt(string(msg.Values[0]), 10, 32)
			if parseErr != nil {
				return nil, fmt.Errorf("parse tablespace OID: %w", parseErr)
			}
			tablespace := pglogrepl.BaseBackupTablespace{OID: int32(oid), Location: string(msg.Values[1])}
			if msg.Values[2] != nil {
				size, parseErr := strconv.Atoi(string(msg.Values[2]))
				if parseErr != nil {
					return nil, fmt.Errorf("parse tablespace size: %w", parseErr)
				}
				tablespace.Size = int8(size)
			}
			tablespaces = append(tablespaces, tablespace)
		case *pgproto3.CommandComplete:
			return tablespaces, nil
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
		case *pgproto3.ErrorResponse:
			return nil, pgconn.ErrorResponseToPgError(msg)
		default:
			return nil, fmt.Errorf("BASE_BACKUP tablespaces: unexpected response %T", msg)
		}
	}
}

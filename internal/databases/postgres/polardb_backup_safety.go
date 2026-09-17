package postgres

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// A session lock must outlive both pg_backup_stop and sentinel publication.
// Use the same PGDATABASE for every backup of a cluster: PostgreSQL advisory
// locks are database-local. Never kill the competing backup process.
func (queryRunner *PgQueryRunner) lockPolarDBDirectBackup(ctx context.Context) error {
	var standby bool
	if err := queryRunner.Connection.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&standby); err != nil {
		return fmt.Errorf("check PolarDB backup source role: %w", err)
	}
	if standby {
		return fmt.Errorf("direct PolarDB backup requires the RW primary; cross-node backup coordination on standby is not supported")
	}
	if err := queryRunner.TryGetLock(ctx); err != nil {
		return fmt.Errorf("cannot acquire PolarDB backup lock (use the same PGDATABASE for all backups): %w", err)
	}
	return nil
}

// Reject incomplete uploads as well as complete backups. This check is made
// under the source session lock, after delta naming and before any tar upload.
func checkPolarDBBackupNameAvailable(ctx context.Context, folder storage.Folder, name string) error {
	exists, err := folder.Exists(ctx, name+utility.SentinelSuffix)
	if err != nil {
		return fmt.Errorf("check PolarDB backup %s sentinel: %w", name, err)
	}
	if exists {
		return fmt.Errorf("refusing to overwrite existing PolarDB backup %s; switch WAL on the primary before retrying", name)
	}
	objects, folders, err := folder.GetSubFolder(name).ListFolder(ctx)
	if err != nil {
		return fmt.Errorf("check PolarDB backup %s objects: %w", name, err)
	}
	if len(objects) != 0 || len(folders) != 0 {
		return fmt.Errorf("refusing to overwrite incomplete PolarDB backup %s; switch WAL on the primary before retrying", name)
	}
	return nil
}

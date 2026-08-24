# Experimental direct PolarDB backup

This branch prototypes a parallel physical backup path for PolarDB for
PostgreSQL. Unlike remote `BASE_BACKUP`, relation files are read directly from
PFSD while the database is inside `pg_backup_start()` / `pg_backup_stop()`.
The resulting backup uses the normal WAL-G tar and metadata format, so the
existing `backup-fetch` and `wal-fetch` commands restore it.

Build the native PFSD implementation on Linux/amd64:

```console
CGO_ENABLED=0 go build -tags pfsnative -o wal-g-pg-polardb ./main/pg
```

Run `backup-push` with the local compute-node data directory as its argument
and the shared-data PFS path in `WALG_POLARDB_PFS_DATA_PATH`:

```console
export WALG_POLARDB_PFS_DATA_PATH=/vdb/polar/shared_data
export WALG_PFS_HOST_ID=2
export WALG_UPLOAD_CONCURRENCY=8
export WALG_S3_PREFIX=s3://example/polardb
wal-g-pg-polardb backup-push "$PGDATA"
```

The direct source is mounted read-only. Multiple tar workers can issue PFSD
reads concurrently and upload/compress different files in parallel. Local
compute-node files keep their normal paths; shared files are stored below
`polar_shared_data/`. The shared `global/pg_control` is uploaded last as the
backup sentinel.

The path excludes `pg_wal`, `pg_logindex`, and `polar_fullpage`.
`polar_enable_switch_wal_in_backup` must be enabled in the server
configuration so `pg_backup_stop()` does not wait for a 1 GiB segment to fill;
PolarDB 17 does not allow WAL-G to change this parameter in a session. The
backup is rejected when both `full_page_writes` and data checksums are
disabled.

For direct backups WAL-G calls `pg_backup_stop(false)` on PostgreSQL 15+ (as it
already does with `pg_stop_backup(false)` on 9.6-14), so completion does not
wait for the required WAL segment to reach the archive. WAL archiving must
remain configured and monitored: the base backup is not recoverable until its
stop-LSN WAL is present. WAL-G logs the number and total size of files found in
the PFS root and rejects a root without `global/pg_control` or non-empty files.

The same `WALG_POLARDB_PFS_DATA_PATH` setting makes `wal-push` read an archive
source path below the shared-data root through PFSD. Set upload concurrency to
one in `archive_command`, because background WAL discovery requires a POSIX
directory and the PolarDB WAL directory is not mounted into the host namespace:

```conf
archive_command = 'env WALG_POLARDB_PFS_DATA_PATH=/vdb/polar/shared_data WALG_PFS_HOST_ID=2 WALG_UPLOAD_CONCURRENCY=1 WALG_FILE_PREFIX=/backup wal-g-pg-polardb wal-push %p'
```

Current scope is full backups through the native-Go PFSD transport. Delta
backup and the C SDK source adapter are intentionally left for comparison
after the full-backup E2E and throughput measurements.

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

The path excludes `pg_wal`, `pg_logindex`, and `polar_fullpage`. It enables
`polar_enable_switch_wal_in_backup` for the backup session so
`pg_backup_stop()` does not wait for a 1 GiB segment to fill. The backup is
rejected when both `full_page_writes` and data checksums are disabled.

Current scope is full backups through the native-Go PFSD transport. Delta
backup and the C SDK source adapter are intentionally left for comparison
after the full-backup E2E and throughput measurements.

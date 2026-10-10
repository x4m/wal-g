# Experimental direct PolarDB backup

This branch prototypes a parallel physical backup path for PolarDB for
PostgreSQL. Unlike remote `BASE_BACKUP`, relation files are read directly from
PFSD while the database is inside `pg_backup_start()` / `pg_backup_stop()`.
The resulting backup uses the normal WAL-G tar and metadata format, so the
existing `backup-fetch` and `wal-fetch` commands restore it.

Build the C SDK implementation on Linux with PFSD headers and `libpfsd.a`
installed under `/usr/local/polarstore/pfsd`:

```console
CGO_ENABLED=1 go build -tags pfs -o wal-g-pg-polardb ./main/pg
```

Alternatively, build the native Go PFSD implementation without the C SDK:

```console
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -tags pfsnative -o wal-g-pg-polardb ./main/pg
```

Both implementations provide the direct backup, restore, `wal-push`, and
`wal-fetch` paths described below. The `pfs` and `pfsnative` tags are mutually
exclusive. Neither implementation is part of regular WAL-G builds.

Run `backup-push` with the local compute-node data directory as its argument
and the shared-data PFS path in `WALG_POLARDB_PFS_DATA_PATH`:

```console
export WALG_POLARDB_PFS_DATA_PATH=/vdb/polar/shared_data
export WALG_PFS_HOST_ID=2
export WALG_UPLOAD_CONCURRENCY=8
export WALG_S3_PREFIX=s3://example/polardb
wal-g-pg-polardb backup-push "$PGDATA"
```

As with other WAL-G settings, these values can instead be placed in the WAL-G
configuration file, for example:

```json
{
  "WALG_POLARDB_PFS_DATA_PATH": "/vdb/polar/shared_data",
  "WALG_PFS_CLUSTER": "disk",
  "WALG_PFS_HOST_ID": 2,
  "WALG_PFSD_SERVER_ADDR": "/var/run/pfsd-vdb.socket"
}
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

Direct backups preserve PostgreSQL's archive wait by default. Set
`WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE=false` in the environment or configuration
file to opt out. On PG 15+ this selects `pg_backup_stop(false)`; on PG 9.6-14 it
selects `pg_stop_backup(false, false)` (the first boolean means non-exclusive).
This setting controls archive waiting, not WAL switching. WAL archiving must
remain configured and monitored: the base backup is not recoverable until its
required WAL is present. WAL-G logs the number and total size of files found in
the PFS root and rejects a root without `global/pg_control` or non-empty files.

Direct backup currently requires the RW primary. All direct backup invocations
for a cluster must use the same `PGDATABASE`: a database-local advisory lock
serializes them through sentinel publication. A competing invocation fails
without terminating the first one. Existing backup names (including partial
uploads) are rejected before uploading data. Use a dedicated repository prefix
per cluster. Standby/cross-compute coordination is not supported yet.

The same `WALG_POLARDB_PFS_DATA_PATH` setting makes `wal-push` read an archive
source path below the shared-data root through PFSD. Set upload concurrency to
one in `archive_command`, because background WAL discovery requires a POSIX
directory and the PolarDB WAL directory is not mounted into the host namespace:

```conf
archive_command = 'env WALG_POLARDB_PFS_DATA_PATH=/vdb/polar/shared_data WALG_PFS_HOST_ID=2 WALG_UPLOAD_CONCURRENCY=1 WALG_FILE_PREFIX=/backup wal-g-pg-polardb wal-push %p'
```

For restore, choose a new, empty shared-data root and run the normal
`backup-fetch`. WAL-G streams shared-file contents from the decompressed tar
archives directly through PFSD. Only compute-local files are written to the
destination directory; no `polar_shared_data` tree is unpacked on local disk.
Both native Go and cgo use this path. `WALG_DOWNLOAD_CONCURRENCY` controls
archive extraction concurrency; memory buffers are bounded per worker.

```console
export WALG_POLARDB_PFS_DATA_PATH=/vdb/polar/restored_shared_data
export WALG_PFS_CLUSTER=disk
export WALG_PFS_HOST_ID=3
export WALG_FILE_PREFIX=/backup
wal-g-pg-polardb backup-fetch /var/lib/polardb/restored LATEST
```

The PFS destination must not exist or must be empty. This prevents an
incomplete restore from being mixed with an older cluster.

The destination is mounted and checked before any tar extraction, so a mount
configuration error no longer leaves a full local shared-data copy behind.
Files are written under temporary names on PFS and renamed after successful
write/close. An exclusive `.wal-g-restore` directory claims the destination
against another restore. The 8-KiB shared `global/pg_control` is held in memory
and published only after all archives have been extracted successfully.
This is not an atomic replacement of an existing cluster and does not add
durability guarantees beyond the installed PFSD implementation.

On failure, local node files and incomplete PFS contents are retained for
diagnosis; automatic rollback or resume is not implemented. Use fresh empty
local and PFS destinations for another attempt. Never start a node from a
failed restore or remove an active node's files to retry.

Streaming restore supports full direct backups and the full-scan deltas
described below. Tablespaces, link/special-file archive entries, `--mask`,
`--restore-only`, `--restore-spec`, reverse unpack and redundant-tar skipping
are rejected rather than falling back to local staging. Normal PostgreSQL
restore without `WALG_POLARDB_PFS_DATA_PATH` is unchanged. No archive-format
change is needed for existing full direct backups.

## Full-scan delta backups

Use the ordinary PostgreSQL WAL-G interface:

```console
export WALG_DELTA_MAX_STEPS=5
export WALG_DELTA_ORIGIN=LATEST  # or LATEST_FULL
wal-g-pg-polardb backup-push "$PGDATA" --full
# After changes, without --full:
wal-g-pg-polardb backup-push "$PGDATA"
# Set WALG_POLARDB_PFS_DATA_PATH to a fresh restore root before fetching:
wal-g-pg-polardb backup-fetch /var/lib/polardb/restored LATEST
```

Base selection, `--delta-from-name`, chain limits and `_D_` names use the normal
PostgreSQL machinery. Numeric main-fork relation segments in shared `base/`
are scanned using the parent's **backup-start LSN**, including zero/new pages.
Files not present in the parent, unsupported forks, global files, SLRUs and
changed compute-local files are copied whole. Compute-local files retain the
ordinary PostgreSQL mtime-based skip optimization. Shared files are never
skipped merely because their mtime matches: PFS may leave it unchanged on
in-place writes without size or allocation changes. The standard `wi1`
page-delta format records the new file
length and selected blocks. Invalid page headers fall back to a whole-file
copy through PFSD; actual read/transport errors fail the backup.

This reduces stored/uploaded bytes, not source reads. Selection performs one
bounded sequential pass; emitting selected pages uses a second sequential
pass up to the last selected page. No seek extension or local data staging is
needed in either SDK. Growth after stat is excluded and concurrent EOF is
zero-padded, as in direct full backups; WAL recovery remains mandatory.

The first implementation requires `full_page_writes=on`, the regular tar
composer, file metadata, matching database identity/server version and the
same timeline. After promotion take a new `--full` backup. Both
`WALG_USE_WAL_DELTA` and `WALG_FORCE_WAL_DELTA` must be false; WAL summaries
and ptrack are not used. The sentinel marks these deltas with
`PolarDBFullScanDelta`; older experimental/unmarked direct deltas are rejected.

Restore validates the chain before mounting and assembles only files needed
by the requested backup. Each delta merges inherited and changed pages into
a temporary PFS file and atomically replaces that file after successful close.
File shrinkage is respected, dropped files are not resurrected, and only the
newest backup's control file is published, after the whole chain succeeds.
Restore still needs fresh destinations and sufficient PFS space for both the
inherited file and its temporary replacement. Local staging is never used.

Local tests cover page selection, chain reconstruction, growth/truncation,
retry, malformed deltas and missing parents. Native Go and cgo passed real
full/delta/delta backup, PITR and heap/index checks, recorded with their
environment prerequisites in [the delta validation](POLARDB_DELTA_VALIDATION.md).
Repeat the real-server test with `POLAR_E2E_DELTA=true` for your environment
(see `docker/polardb_tests/README.md`); unit tests alone do not establish
hot-backup/PITR correctness.

Native Go and cgo restore/PITR checks are recorded in
[the streaming restore validation](POLARDB_STREAMING_RESTORE_VALIDATION.md).

During archive recovery, `wal-fetch` maps the basename of PostgreSQL's `%p`
destination to `$WALG_POLARDB_PFS_DATA_PATH/pg_wal/`. It downloads and
decompresses the complete WAL segment, writes it through PFSD under a
temporary name, and atomically renames it into place. For example, the normal
`pg_wal/RECOVERYXLOG` destination is published as
`$WALG_POLARDB_PFS_DATA_PATH/pg_wal/RECOVERYXLOG`:

```conf
restore_command = 'env WALG_POLARDB_PFS_DATA_PATH=/vdb/polar/restored_shared_data WALG_PFS_CLUSTER=disk WALG_PFS_HOST_ID=3 WALG_FILE_PREFIX=/backup wal-g-pg-polardb wal-fetch %f %p'
```

Both clients chunk large reads and writes to requests of at most 1 MiB,
which is compatible with both the public SDK and the validated PFSD fork.
The C SDK mount lifecycle is process-global. The native client does not use
the SDK global state, but PFSD still permits only one live process for each
PBD/host-ID pair.

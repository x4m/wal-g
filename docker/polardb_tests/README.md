# PolarDB backup/restore E2E and comparison

`scripts/run_e2e.sh` runs the same logical backup/restore test twice:

1. remote PostgreSQL `BASE_BACKUP` (`wal-g backup-push`);
2. parallel direct PFSD backup (`wal-g backup-push "$PGDATA"`).

Both backups are fetched, installed into independent PFS locations, started as
PolarDB RW nodes and checked with the same deterministic SQL digest. Wall-clock
measurements and `backup-list --json` output are saved in the results directory.

The test intentionally runs on a Linux host instead of a normal Docker
container. PFSD needs a running daemon, a PBD/block device, host IDs and usually
privileged setup. Those environment-specific operations are supplied as
executable hooks in `POLAR_E2E_HOOKS`:

| Hook | Arguments | Contract |
|---|---|---|
| `preflight` | none | Check Linux, PFSD, PolarDB and test device; make no changes. |
| `reset_source` | none | Recreate the disposable source PFS and local node. |
| `wait_backup_ready` | mode | Wait until the backup's stop-LSN WAL is present in the repository. |
| `reset_restore` | mode, fetched directory | Empty the mode's disposable local node and dedicated PFS restore root. |
| `restore_pfs_data_path` | mode | Print the dedicated `/<pbd>/<path>` used by direct `backup-fetch`. |
| `prepare_restore` | mode, fetched directory | Install local node files and create `recovery.signal`; direct shared data has already been copied to PFS by WAL-G. |
| `start_restore` | mode | Start the node, complete recovery, promote it and wait for read-write SQL readiness. |
| `stop_restore` | mode | Stop that node; it should be safe when already stopped. |
| `digest` | mode, SQL | Run SQL and print only its unaligned scalar result. |

Skeletons for all hooks are in `hooks/example`; they fail closed until their
environment-specific commands are implemented.

The lifecycle hooks own destructive operations, which makes their target block
device and directories reviewable for each environment. In `direct_pfsd` mode,
the runner sets `WALG_POLARDB_PFS_DATA_PATH` for both `backup-push` and
`backup-fetch`; copying `polar_shared_data` in a hook would bypass the storage
implementation under test. The runner itself only removes its
`POLAR_E2E_WORK_DIR` subdirectories and backup objects below two dedicated
prefixes.

Required environment:

```console
export WALG_BIN=/opt/wal-g/bin/wal-g-pg-polardb
export POLAR_E2E_HOOKS=/opt/polardb-e2e-hooks
export POLAR_SOURCE_PGDATA=/var/lib/polardb/source
export POLAR_SOURCE_PFS_DATA_PATH=/vdb/walg-e2e-source
export POLAR_E2E_STORAGE_ROOT=/var/lib/wal-g/polardb-e2e
export WALG_PFS_HOST_ID=20
docker/polardb_tests/scripts/run_e2e.sh
```

`WALG_BIN` must be built on Linux with `-tags pfsnative`. The source PolarDB
must have `full_page_writes=on` or data checksums, and
`polar_enable_switch_wal_in_backup=on`. Use different PFSD host IDs for
concurrent processes if required by the installed PFSD version.

Optional variables include `POLAR_E2E_RESULTS_DIR`, `POLAR_E2E_WORK_DIR`,
`POLAR_E2E_LOAD_SQL`, `POLAR_E2E_ROLLBACK_SQL`,
`POLAR_E2E_CHECKPOINT_SQL`, `POLAR_E2E_DIGEST_SQL`,
`POLAR_E2E_ROLLBACK_CHECK_SQL`, and `POLAR_E2E_RECOVERY_CHECK_SQL`. The default
dataset has 100,000 deterministic rows and a separately rolled-back row. Every
restored node must be promoted (`NOT pg_is_in_recovery()`) and must preserve
both the digest and rollback result. For throughput runs, override the load SQL
with a larger dataset and repeat with selected `WALG_UPLOAD_CONCURRENCY`
values. The execution order is configurable with `POLAR_E2E_MODES`; benchmark
runs should be repeated in both orders (and after an explicit host-specific
cache reset) to avoid giving the second method an advantage from a warm PFS
cache.

PolarDB commonly uses 1 GiB WAL segments. `pg_backup_stop(false)` makes the
base backup finish before its stop-LSN segment is necessarily archived, so the
`wait_backup_ready` hook is part of correctness rather than just timing. Do not
start restore while the required WAL object is still a temporary upload.

This first harness measures end-to-end time. CPU, PFSD byte counters and peak
RSS should be added after the lifecycle is stable on the target CI machine;
they are host-specific and must not affect the correctness verdict.

The orchestration itself has a dependency-free self-test:

```console
docker/polardb_tests/scripts/test_runner.sh
```

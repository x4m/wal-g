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
| `prepare_restore` | mode, fetched directory | Copy fetched `polar_shared_data` to a fresh PFS and install local node files. |
| `start_restore` | mode | Start `source`, `base_backup`, or `direct_pfsd` and wait until SQL is available. |
| `stop_restore` | mode | Stop that node; it should be safe when already stopped. |
| `digest` | mode, SQL | Run SQL and print only its unaligned scalar result. |

Skeletons for all hooks are in `hooks/example`; they fail closed until their
environment-specific commands are implemented.

The lifecycle hooks own destructive operations, which makes their target block
device and directories reviewable for each environment. The runner itself only
removes its `POLAR_E2E_WORK_DIR` subdirectories and backup objects below two
dedicated prefixes.

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
`POLAR_E2E_LOAD_SQL`, and `POLAR_E2E_DIGEST_SQL`. The default dataset has
100,000 deterministic rows. For throughput runs, override the load SQL with a
larger dataset and repeat with selected `WALG_UPLOAD_CONCURRENCY` values. The
execution order is configurable with `POLAR_E2E_MODES`; benchmark runs should
be repeated in both orders (and after an explicit host-specific cache reset) to
avoid giving the second method an advantage from a warm PFS cache.

This first harness measures end-to-end time. CPU, PFSD byte counters and peak
RSS should be added after the lifecycle is stable on the target CI machine;
they are host-specific and must not affect the correctness verdict.

The orchestration itself has a dependency-free self-test:

```console
docker/polardb_tests/scripts/test_runner.sh
```

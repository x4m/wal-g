# Streaming PolarDB restore validation, 2026-09-26 UTC

Branch: `polardb-direct-native-go`, working-tree changes after `eb0b233d`.
These are experimental correctness results, not production certification or a
throughput comparison.

## Scope

`backup-fetch` now extracts `polar_shared_data/*` directly through PFSD.
It keeps compute-local files local, uses bounded per-worker buffers, and
publishes shared `global/pg_control` only after successful archive extraction.
The old extract-locally-then-copy implementation has been removed for both
native Go and cgo builds. No archive-format change is required.

Tests used the Linux/amd64 disposable VM, PolarDB 17 shared-storage mode, a
socket/memfd PFSD build, and a VFS linked to its matching SDK. The archive
repository was a local FS directory; shared database files were read/written
through PFSD, not FUSE. S3 performance was not tested.

This run restored an existing real full backup,
`base_000000020000000C00000001`, from the September 15 concurrent-writer
scenario described in [the previous validation](POLARDB_DIRECT_VALIDATION.md).
That backup overlapped 29 committed writer transactions, and its required WAL
was uploaded by `archive_command`. No new backup-push or manual WAL upload
was performed in this restore-focused run.

## Real restore and PITR

Both binaries restored the same archive into separate fresh PFS roots and
local compute directories. Each node recovered to the named restore point
`walg_archive_round_2`, which includes changes committed after backup-stop.

| Check | Native Go (`CGO_ENABLED=0`, `pfsnative`) | cgo (`pfs`, `libpfsd.a`) |
|---|---|---|
| Streaming fetch and startup | Passed | Passed |
| No local `polar_shared_data` observed during fetch | Passed | Passed |
| Maximum sampled local directory allocation during fetch | 1,464 KiB | 1,460 KiB |
| Complete 120,000-row SQL digest | Matched | Matched |
| Rolled-back row absent | Passed | Passed |
| Named PITR and promotion to RW | Passed | Passed |
| Nonempty PFS target rejected before local extraction | Passed | Passed |

Expected and observed digest in both cases:

```text
120000:bd7b4a5031f52798ecea8d8ce7f794a4
```

The local directory was sampled every 100 ms throughout fetch. This is a
staging regression check, not an RSS measurement or proof of performance on
large datasets. The repository's archive E2E runner now observes the directory
during fetch and requires the streaming completion message as well.

Tested executable SHA-256:

```text
native d04dd8cd71cda70297e5d2d637e553b65b286544680252e9ca6c34030ae0b209
cgo    6f69bea006f710cb1bd660dff39ecac2771cce4f5ca54e4220b73374a451f97d
```

## Local checks

The PostgreSQL, configuration and utility suites passed; `cmd/pg` compiled.
Focused streaming-restore tests passed with the race detector, and `go vet`
passed for the PostgreSQL and command packages. Tests cover local/shared
routing, concurrent writes, replay of an archive entry, exclusive restore
claims, short/read/close/rename failures, unsafe paths and links, cancellation
with an active writer, late callbacks after shutdown, invalid/conflicting
control files, corrupted tar data after a valid control-file payload, and
preflight rejection of unsupported backups or mount failures.

Commands used the local toolchain's `GOEXPERIMENT=jsonv2`, `-mod=mod`, a
temporary Go cache and a short `TMPDIR` for Unix socket tests:

```console
go test -mod=mod ./internal/databases/postgres ./internal/config ./utility ./cmd/pg
go test -mod=mod -race ./internal/databases/postgres -run '^TestPolarRestore' -count=1
go vet -mod=mod ./internal/databases/postgres ./cmd/pg
bash -n docker/polardb_tests/scripts/run_archive_e2e.sh
bash docker/polardb_tests/scripts/test_runner.sh
```

The last command validates the original orchestration self-test, not the real
archive lifecycle. The September 26 tests above independently exercised
restore/PITR against a retained concurrent-writer backup. The full fresh-backup
runner was subsequently executed on September 27, as recorded below.

## Environment issues and retained evidence

The first attempts found missing PFSD runtime directories/lock permissions
after a VM reboot. After repairing that fixture, native fetch succeeded but
recovery failed during promotion with `ENOSPC`: old restore copies had filled
PFS. Two stopped September 15 restore copies (`walg-sep15b-archive_1` and
`walg-sep15b-archive_2`) and the failed September 26 restore root were removed.
Their backup repositories and diagnostic logs were preserved; the removed
copies can be recreated from those archives. The successful native run was a
fresh restore, not a continuation of the failed recovery.

Successful results are retained on the VM under
`/tmp/walg-sep26-native-results` and `/tmp/walg-sep26-cgo-results`, including
fetch/recovery logs, SQL digests, nonempty-target errors and local-size samples.
The source cluster and archive repositories were not modified by this run.
Both restored nodes and the temporary daemon were stopped. The original VFS
was restored byte-for-byte, and temporary private source, SDK and linked
artifacts were removed. The unrelated system PostgreSQL instance remained
running. An evidence bundle is retained as `/tmp/walg-sep26-evidence.tar.gz`.

## Current boundaries

Streaming restore requires a full direct backup and fresh destinations.
Incrementals, tablespaces, links/special entries, masks, partial restore,
reverse unpack and redundant-tar skipping are explicitly unsupported.
Automatic rollback/resume after a failed restore is not implemented.
Temporary-file rename and delayed control publication do not strengthen the
installed PFSD implementation's durability guarantees. Normal PostgreSQL
restore without `WALG_POLARDB_PFS_DATA_PATH` retains its existing path.

## Full fresh-backup E2E follow-up, 2026-09-27 UTC

Both complete `run_archive_e2e.sh` scenarios passed with the streaming restore
implementation: native Go from 05:22:18 to 05:31:18 UTC, cgo from 05:31:32 to
05:40:07 UTC. These times describe fixture runs, not throughput benchmarks.

Each implementation used an empty, separate archive repository and created
two new full backups. The second backup ran alongside continuous committed
updates (46 writer commits for native Go, 47 for cgo). Each backup was then
restored directly through PFSD into its own fresh shared-data root and
recovered to a named target containing changes committed after backup-stop.

| Client | New backup | Restored row count and digest |
|---|---|---|
| Native Go | `base_000000030000000C00000003` | `120000:bd245222f6f717b7632968b415bf95be` |
| Native Go, concurrent writer | `base_000000030000000D00000002` | `120000:fd3188a5f97da1514a5441fbf2534a5d` |
| cgo | `base_000000030000000E00000002` | `120000:bd245222f6f717b7632968b415bf95be` |
| cgo, concurrent writer | `base_000000030000000F00000001` | `120000:de8cc6c2024d8eedee0a2ddad26bb833` |

All four digests matched the values recorded on the source at the corresponding
targets. All four restores excluded the rolled-back row, promoted to RW, and
stopped normally. Both full runs additionally passed:

- `WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE=true`, automatic `archive_command` and
  shared `.done` checks; no manual WAL uploads;
- distinct backup names and SHA-256 preservation of every first-backup object,
  including metadata and the completion sentinel;
- rejection of a competing SQL backup lock before a third backup appeared;
- observation throughout fetch of no local `polar_shared_data` staging tree,
  plus the explicit streaming-restore completion message.

The scenario used the same Linux/amd64 socket/memfd PFSD fixture and FS archive
backend as above. A retained disposable September 26 restore became the RW
test source; the original source cluster and earlier backup repositories were
not changed. The previously prepared public
`pgarch-pfs-status-rename.patch` was temporarily applied to the test server.
This separately patched server is a prerequisite for archive-wait testing on
this version, not a change made by WAL-G.

The actual executed runner matched the repository copy byte-for-byte:

```text
runner 0f71f6057881a1db46b7e8521dc9c67454c6b78f72e8d68f1887f76ff7048702
native d04dd8cd71cda70297e5d2d637e553b65b286544680252e9ca6c34030ae0b209
cgo    8930be74857953d5f1d2def6fceaadddd92808d377f29ac9122050d05e5f5eb2
```

Logs and source/restore digests are under `/tmp/walg-sep27-native-results` and
`/tmp/walg-sep27-cgo-results` on the test VM; an evidence bundle is retained as
`/tmp/walg-sep27-evidence.tar.gz`. New archives are in the corresponding
`/home/x4mmm/walg-sep27-{native,cgo}-repository` directories.

To make room, five stopped, reproducible old restore copies were removed:
`walg-sep15d-archive_1`, `walg-sep15d-archive_2`, `walg-sep15e-archive_1`,
`walg-sep15e-archive_2`, and `walg-sep26-stream-cgo` under `/vdb/`. Their original
backup repositories and diagnostic evidence were preserved. The four newly
validated restore roots are retained.

After both full runs, all test nodes and the temporary daemon were stopped.
The original server source/object, backend and installed executables, and VFS
were restored and compared byte-for-byte. Temporary private SDK sources and
linked test artifacts were removed. The unrelated system PostgreSQL instance
remained running.

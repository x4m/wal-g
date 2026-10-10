# PolarDB full-scan delta validation, 2026-10-10 UTC

Branch: `polardb-direct-native-go`, working-tree changes after `b55cc2a3`.
These are correctness checks on a small disposable dataset, not a performance
comparison or production certification.

## Environment and prerequisites

The replacement Linux/amd64 test host ran isolated Ubuntu 22.04 containers,
PolarDB 17.11 (`POLARDB_17_STABLE`, public revision `fd9fc37`), and a
socket/memfd PFSD build with its matching C SDK. Database files resided on a
dedicated loop-backed PFS volume, not on a POSIX/FUSE mount. The backup and WAL
archive repository used the FS storage backend. This does not test S3.

The server used data checksums, `full_page_writes=on`, 64 MiB WAL segments,
`polar_enable_switch_wal_in_backup=on`, and the existing public
`pgarch-pfs-status-rename.patch`. That server patch is required on the tested
revision for shared `.ready` to `.done` transitions and backup-stop archive
waiting. The test explicitly set `WALG_PG_WAL_SIZE=64`.

The test PFSD build also needed an initialization-order fix under GCC 11 /
C++20: its attribute constructor initialized mount locks before their C++
dynamic initialization, which subsequently reset their state. Without this
fixture-only fix, even the formatting CLI crashed. These results therefore
do not establish compatibility with that unmodified PFSD build. Private PFSD
source and the fix are not included in this repository or published.

The dedicated sparse volume was expanded from 20 to 40 GiB to accommodate
the source and three independent restores, including PFS inode/allocation
overhead. No existing host volume was formatted.

## Native Go result

`CGO_ENABLED=0`, build tag `pfsnative`: the real archive E2E runner passed
with `POLAR_E2E_DELTA=true`. It created `full -> delta -> delta` and restored
all three backups separately through PFSD to their own named PITR targets.
The target changes were committed after each backup stopped, so successful
recovery required archived WAL, not just extraction.

- All three complete SQL digests matched; rolled-back rows remained absent.
- All three nodes reached the named target and promoted to read/write mode.
- `pg_amcheck --heapallindexed --parent-check --on-error-stop --verbose`
  passed on each restored `postgres` database, including its catalogs.
- The concurrent writer committed eleven transactions, with recorded
  timestamps overlapping the middle hot backup.
- Deletion/recreation, vacuum truncation, subsequent growth and `TRUNCATE`
  were exercised across the chain.
- Each delta contained 733 files marked `IsIncremented`, not merely a `_D_`
  backup name. Both WAL-delta settings were false.
- The first backup's object hashes remained unchanged; a competing backup
  SQL lock was rejected without publishing another completed backup.
- Each fetch wrote shared files directly through PFSD; the runner observed
  no local `polar_shared_data` staging tree during extraction.

Native archive metadata (bytes; not a throughput benchmark):

| Backup | Uncompressed | Compressed |
|---|---:|---:|
| Full, `base_000000010000000000000002` | 48,143,269 | 14,495,953 |
| Delta 1, start segment `000000010000000000000005` | 21,254,214 | 7,975,144 |
| Delta 2, start segment `000000010000000000000008` | 13,560,010 | 3,712,122 |

## cgo result and negative controls

The `pfs` build linked against `libpfsd.a` passed the same fresh
full/delta/delta scenario, all three PITR digest comparisons and promotion
checks, and `pg_amcheck` on all three restored databases. The concurrent
writer committed nine transactions; each delta again contained 733
incremental files. The first archive's hashes remained unchanged, competing
backup rejection passed, and fetch did not stage shared data locally.

Afterward, two real backup attempts on the disposable cgo source verified
that enabling `WALG_USE_WAL_DELTA` or disabling `full_page_writes` produces
the intended explicit rejection. `full_page_writes` was restored to `on`.
There were still exactly three completed backups, and the first backup's
object hashes were unchanged after both failures.

Both client libraries also passed their real PFSD file-operation integration
tests. These checks do not simulate daemon failure or a power loss.

## mtime finding

A separate real-PFSD probe created an 8192-byte file, closed and stat'ed it,
waited two seconds, then overwrote it with different 8192-byte contents.
Reading it back confirmed the change, but its mtime stayed identical.
Consequently shared PFS relation files must be scanned even if their mtime
matches the parent backup. The normal PostgreSQL mtime shortcut is retained
only for compute-local files.

## Local checks and test-harness correction

The PostgreSQL, configuration and utility unit suites passed; `cmd/pg`
compiled. Focused PolarDB/direct tests passed with `-race`; `go vet` passed
for the PostgreSQL and command packages. Shell syntax and the original
orchestration self-test passed. The latter is not a substitute for PFSD E2E.

The first real restore exposed a test-harness race: `pg_ctl -w` returns when
hot standby accepts connections, before named-target recovery necessarily
finishes. The archive runner now waits for `NOT pg_is_in_recovery()` before
comparing digests and retains actual digests for mismatch diagnostics. The
successful native run started again with fresh source/restore directories.

Unit coverage includes page-LSN selection, zero pages, growth/truncation,
invalid-page whole-file fallback, ordinary PG decoding of the `wi1` format,
full/delta/delta reconstruction, inherited local files, dropped-file filtering,
retry, malformed deltas, missing parents and base identity/timeline checks.

## Evidence and boundaries

The native run is retained in the test runtime container under
`/work/runs/native-05`, with runner output `/work/native-05-run.log` and
consistency-check output `/work/native-05-amcheck.log`. The cgo equivalents
are `/work/runs/cgo-01`, `/work/cgo-01-run.log`,
`/work/cgo-01-amcheck.log`, and `/work/cgo-01-guards.log`. After verification,
the stopped native and cgo shared PFS copies were removed to reclaim disk
space. Both backup/WAL repositories, local directories, checksums and logs
were retained, allowing the shared copies to be restored again.
The test databases, PFSD and both dedicated test containers were stopped.

The evidence bundle is retained on the host as
`/home/x4m/walg-polar-e2e-20261010/polar-delta-evidence-20261010.tar.gz`.
It includes the runner and fixture hooks, SQL comparisons, backup/fetch and
recovery logs, `pg_amcheck`, negative controls and client integration results.
Its SHA-256 is
`6d11c5063c56b26089f9fb17230c276b4fabc6c311f81c52928aff35317bb2a4`.

Tested binary SHA-256:

```text
native 1afcbdd296b3fb689391831f1705a451c708aa6ae866990c7a82bbeeedf72422
cgo    b54781e276117b58c102866cab3268c4fa3e546bec21842118dcd9b4cd8a7e0d
```

This run does not establish large-cluster performance, 1 GiB WAL operation,
cross-timeline deltas, tablespace support, daemon crash durability or behavior
on every PFSD/server version. Delta selection saves archived bytes, not the
full source scan; restore also reads ancestor archives and rewrites inherited
PFS files while applying each layer. Take a new full backup after promotion.

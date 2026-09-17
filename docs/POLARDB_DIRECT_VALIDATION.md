# Direct PolarDB validation, 2026-09-15

Working branch: `polardb-direct-native-go`, changes after `650eebd2`.
These are experimental results, not a production-readiness declaration.

## Environment and boundaries

Linux/amd64 test VM, PolarDB 17 with shared data on PFS, a socket/memfd PFSD
build, and a VFS linked to its SDK. The WAL-G archive repository was a local
FS directory; shared source and restore files were accessed through PFSD, not
through FUSE. No S3 performance or production-server compatibility claim is
made by this test.

The initial pass restored the original daemon and VFS and removed temporary
private source, SDK and test binaries without changing server source. A
subsequent, explicitly approved pass tested a separate server patch (below).
Logs, backup objects and stopped restore directories are retained on the
disposable VM for diagnosis.

## Results

| Check | Result |
|---|---|
| PostgreSQL/config/utility unit suites on macOS | Passed; short `TMPDIR` avoids the Unix socket path-length limit. |
| PostgreSQL unit suite, Linux native build | Passed with a 10-minute suite timeout; the earlier 2-minute run timed out. |
| Native client unit suite and race detector on Linux | Passed. |
| Native and cgo filesystem round-trip against socket PFSD | Passed: write, read, stat, list, rename and removal. |
| Final native client against legacy PFSD transport | Passed filesystem round-trip after restoring the original daemon. |
| Native client after real daemon restart | Old session timed out and rejected reuse; close plus fresh mount/stat succeeded. |
| Queue cancellation and late response | Unit tests verified no abandoned queue reservation and no in-flight buffer reuse. |
| Mount locks | Unit tests verified metadata release retains the host-ID reservation and excludes duplicate clients. |
| cgo build against `libpfsd.a` | Passed, including the previously missing `time` import. |
| Concurrent direct backup | Rejected by the SQL session lock before copying. Existing backup objects were unchanged. |
| Existing full/partial backup name | Unit tests reject both sentinel and partial-object collisions. |
| Direct backup on recovery node | Rejected before copying; primary-only coordination is an explicit current limitation. |

The native backup `base_000000020000000500000002` and cgo backup
`base_000000020000000600000001` were both restored into separate fresh PFS
roots. SHA-256 checks verified that creating the second backup did not modify
any object of the first backup. Both runs explicitly used
`WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE=false`.

After each backup, additional changes were committed and named restore
points created. Recovery therefore had to apply WAL beyond the base backup:

| Backup/restore implementation | Target | Restored row count and digest |
|---|---|---|
| Native Go | `walg_nowait_round_1` | `120000:bddfba1c10c55d7b0b3b3072faddca52` |
| cgo | `walg_cgo_committed_target`, LSN `6/C0001448` | `120000:241190bc8bbe56c28e207dd6a54233c4` |

The digest was `md5(string_agg(payload, ',' ORDER BY i))`. Both matched their
expected states, excluded a rolled-back row, and eventually returned
`pg_is_in_recovery() = false`. The cgo recovery was first paused at the target
to exercise the negative recovery-node backup test, then resumed to RW.
Required WAL was uploaded by the source's `archive_command`, not by manual
`wal-push` commands.

The native PITR preceded the final mount-lock lifetime correction; the final
client additionally passed lock tests, socket and legacy filesystem tests,
and the real daemon-restart test. This is not a claim that every final source
change was covered by another complete PITR run.

## Initial server blocker

With the default archive wait enabled, backup-stop hit the configured
90-second statement timeout and did not publish a completion sentinel. On the
tested server (`fd9fc37`), `pgarch_archiveDone` strips the `pfsd://` prefix and
calls `rename` on the resulting path. Runtime logs show `ENOENT` for
`.ready -> .done`; a native SDK listing confirms that `.ready` files remain.
The inspected `pgarch.c` had no local changes. Thus a successful WAL upload
does not complete the server's archive-status lifecycle in this environment.
Fast shutdown of the source also failed to finish; the disposable source was
subsequently stopped in immediate mode.

Repeated archiving exposed a separate WAL-G bug: overwrite checking opened
the source using the local filesystem. It now uses the configured PFSD source
and compares incrementally instead of retaining two 1-GiB files in memory.
Automatic retries progressed after that fix, but it cannot repair the
server's status-file rename.

Those initial runs did not validate the complete `wait=true` archive
lifecycle or concurrent SQL-writer scenario. Positive standby/cross-compute
backup support is not claimed either.

## Separate server patch and stronger repeatable test

`docker/polardb_tests/patches/pgarch-pfs-status-rename.patch` changes only public
PolarDB `pgarch.c`: retain the storage protocol and use `polar_rename()` for
the archive-status transition. It was prepared against `fd9fc37`, built on
the VM and checked against the running server's source. No private SDK code
is included. This is a server-side prerequisite for the affected version,
not a workaround silently applied by WAL-G.

`docker/polardb_tests/scripts/run_archive_e2e.sh` exercises two direct backups
with `WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE=true`, continuous committed SQL writes
during the second backup, SHA-256 preservation of all first-backup objects,
rejection of a competing SQL backup lock, automatic `.done` checks, and PITR
of both backups into distinct fresh PFS roots. Each target includes changes
committed after backup-stop; checks require the complete 120,000-row digest,
absence of a rolled-back row and successful promotion to RW. All required
WAL is uploaded by `archive_command`; none is uploaded manually.

The native executable is Linux/amd64, `CGO_ENABLED=0`, statically linked. The
cgo executable uses `libpfsd.a` and dynamically linked system C/C++ runtimes;
it has no runtime dependency on `libpfsd.so`.

The first stronger native run passed both PITRs. The first cgo run exposed a
shared direct-reader race under the continuous writer: a relation grew after
its tar size was recorded, causing `archive/tar: write too long`. A focused
test reproduced the error before the fix. Direct custom openers now use the
same bounded/zero-padded reading policy as `StartReadingFile`: ignore later
extensions, pad EOF after truncation, and classify deletion before open as a
missing file. Real I/O/transport errors remain errors, not zero padding. The
change affects both native and cgo direct backups; the regular local-file
path is unchanged. Regression tests cover growth, truncation, deletion,
reader closure and error propagation. PostgreSQL/config/utility suites on
macOS and focused Linux cgo regressions passed after this fix.

### Final full runs after the direct-reader fix

Both full archive/PITR scenarios passed on September 15 (UTC); results were
rechecked on September 16. These runs used the separately patched server.

| Client | Backup | Restored row count and digest |
|---|---|---|
| cgo | `base_000000020000000A00000000` | `120000:bd245222f6f717b7632968b415bf95be` |
| cgo, concurrent writer | `base_000000020000000A00000003` | `120000:8ccf1a9cdda17b98125c28eaf5a55049` |
| Native Go | `base_000000020000000B00000002` | `120000:bd245222f6f717b7632968b415bf95be` |
| Native Go, concurrent writer | `base_000000020000000C00000001` | `120000:bd7b4a5031f52798ecea8d8ce7f794a4` |

All four matched the saved source digests at their named recovery targets,
excluded the rolled-back row, promoted to RW and stopped normally. The second
cgo/native backups overlapped 27/29 committed writer transactions,
respectively. Both runs checked automatic archive completion, distinct backup
names, unchanged first-backup objects and rejection of a competing backup.
Results on the VM are under `/tmp/walg-sep15d-results` (cgo) and
`/tmp/walg-sep15e-results` (native); the earlier failed cgo run is retained
under `/tmp/walg-sep15c-results`.

Tested executable SHA-256 values:

```text
cgo    6dfa933a860a371a1ddc34d393d4ce77270804ce38e24b3208cf489a1afa87ed
native 0492d70fde3c57be9e53633abb0d14d6c36f7a19c668c9add7bc420ec2182729
```

The first patched-server run had to drain old `.ready` files and took about
160 seconds inside backup-stop. Later stop waits were about 12 seconds in
this small-data test. These are correctness observations, not representative
throughput measurements or production timeout recommendations.

The source subsequently stopped in fast mode in 11 seconds. On September 16,
the original server executable, backend executable/object/source, VFS and
legacy PFSD daemon were restored; binary comparisons verified the installed
server and VFS. The public server patch still passes `git apply --check`
against the restored source. Temporary private SDK sources, archives and
statically SDK-linked test executables were removed; the original private
checkout was untouched. An evidence archive containing test logs/results (no
SDK source) was retained as `/tmp/walg-sep15b-evidence.tar.gz` locally and on
the VM. Backups and stopped restore directories were retained as well.

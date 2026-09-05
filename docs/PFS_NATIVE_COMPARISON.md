# PFSD C SDK and native Go comparison

This document records a preliminary comparison of WAL-G's two direct PFSD
storage implementations:

- `pfs`: upstream C SDK through cgo;
- `pfsnative`: native Go implementation of the PFSD v2 shared-memory protocol.

## Functional comparison

| Area | C SDK (`pfs`) | Native Go (`pfsnative`) |
|---|---|---|
| WAL-G PostgreSQL backup/restore E2E | Passed | Direct path pending revalidation |
| Build dependency | C/C++ toolchain and installed PFS SDK | Go toolchain only |
| Resulting Linux binary | Dynamically linked to glibc | Statically linked |
| Cross-compilation | Constrained by cgo and PFS SDK | Linux/amd64 from any Go build host |
| Supported platform | Linux with compatible PFS SDK | Linux/amd64 with validated glibc PFSD ABI |
| PFSD ABI ownership | Upstream SDK | Maintained in the Go module |
| In-process recovery after mount rejection | Unsafe because SDK state is process-global | Bounded reconnect is supported |
| Mutating timeout result | Classified as ambiguous | Classified as ambiguous |
| Concurrency | Upstream SDK channels | Parallel reads; mutations serialized per client |

Both implementations require distinct PFSD host IDs for simultaneous
processes accessing the same PBD. PostgreSQL restore should use `wal-g daemon`
and `walg-daemon-client`; expected probes for missing history/WAL files can
otherwise terminate a short-lived process before it unmounts PFSD.

## Preliminary runtime benchmark

Environment:

- temporary Ubuntu 24.04 x86-64 VM;
- the same running `pfsdaemon` and PBD (`vdb`);
- uncompressed 64 MiB random file;
- three repetitions for bulk I/O, ten for metadata/startup;
- separate storage sub-prefixes and PFSD host IDs;
- elapsed time measured around the complete WAL-G process, including
  mount/unmount;
- every downloaded file verified byte-for-byte with `cmp`.

The table reports medians:

| Workload | C SDK | Native Go | Native difference |
|---|---:|---:|---:|
| Empty listing / process startup | 0.135 s | 0.140 s | +4% |
| Recursive backup-prefix listing | 0.120 s | 0.175 s | +46% |
| Upload 64 MiB | 15.11 s (4.24 MiB/s) | 17.17 s (3.73 MiB/s) | +14% elapsed |
| Download 64 MiB | 3.14 s (20.38 MiB/s) | 4.71 s (13.59 MiB/s) | +50% elapsed |
| Upload peak RSS | 49.5 MiB | 70.6 MiB | +43% |
| Download peak RSS | 49.4 MiB | 70.5 MiB | +43% |

The native implementation is already comparable for mount latency but has a
clear bulk-I/O penalty. Its current request path copies the shared-memory I/O
buffer into a temporary Go slice and then into the caller's buffer. It also
polls atomic completion state instead of blocking on the C SDK semaphore.
Removing the extra data copy is the first optimization to evaluate; polling
strategy and per-client serialization are the next candidates.

These numbers are directional rather than production capacity figures. The
VM's PFS write throughput was only about 4 MiB/s and the cgo runtime binary was
an earlier build of the same storage driver because a cold rebuild of the full
WAL-G dependency graph did not finish during the benchmark window. A final
decision should use identical release builds, production-like PFS hardware,
larger samples, concurrent workers, and daemon restart/failure injection.

## Reproduction outline

Build the alternatives independently:

```sh
CGO_ENABLED=1 go build -tags pfs -o wal-g-pg-pfs ./main/pg
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -tags pfsnative -o wal-g-pg-pfsnative ./main/pg
```

For each binary, use an isolated `WALG_STORAGE_PREFIX` and host ID, then time
the same commands:

```sh
wal-g st put source-64m.bin object.bin --no-compress
wal-g st get object.bin result.bin
cmp source-64m.bin result.bin
wal-g st ls pg-backup-prefix/ --recursive
```

Record elapsed time, user/system CPU, maximum RSS, PFSD daemon errors, stale
pidfiles, and checksums. Run cgo and native samples in alternating order for a
more rigorous follow-up to reduce cache and temporal bias.

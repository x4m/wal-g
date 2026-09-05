# Native Go PFSD client

This package is an experimental, cgo-free implementation of the PFSD shared
memory transport. It implements the connection lifecycle and the filesystem
operations needed by WAL-G: stat, directory listing and mutation, open,
sequential read/write, rename, and removal.

1. create and lock the SDK pidfile;
2. send the mount request using the PFSD v2 binary layout;
3. wait for and validate the daemon acknowledgement;
4. map and validate all seven shared-memory regions;
5. signal unmount, close the pidfile, and wait for daemon cleanup.

It is wired into WAL-G with the `pfsnative` build tag and supports direct
PolarDB backup, restore, `wal-push`, and `wal-fetch`. The request path uses
atomic request ownership, epochs, cancellation via `REQ_ZOMBIE`, and
response-state polling. PFSD's own
SDK documents the response semaphore as a CPU-usage optimization; completion
is published through the atomic `REQ_WAIT_RELEASE` state, so the native client
does not call into glibc to wait. Mutating calls are serialized per client;
independent reads can use separate request slots concurrently.

Mutating calls are never automatically replayed after publication. Transient
write, rename, and directory-operation failures are marked ambiguous because
the daemon may have completed them before the client lost the response.

The client is intentionally restricted to Linux/amd64. The pidfile protocol
itself is simple, but the mapped structures use the platform C ABI (`size_t`,
`sem_t`, atomics, and alignment). Supporting another architecture requires ABI
validation against the exact PFSD build.

Run the real-daemon test with:

```sh
PFSNATIVE_TEST_PBD=vdb \
PFSNATIVE_TEST_HOST_ID=14 \
PFSNATIVE_TEST_SERVER_DIR=/var/run/pfsd/vdb \
PFSNATIVE_TEST_STAT_PATH=/vdb/wal-g-integration \
PFSNATIVE_TEST_READ_PATH=/vdb/wal-g-integration/path/to/file \
PFSNATIVE_TEST_READ_SHA256=expected-digest \
go test -mod=mod ./pkg/pfsnative -run TestMountIntegration -v
```

Use a host ID reserved for the test. The client acquires the same
`/var/run/pfs/<pbd>-paxos-hostid` range locks as the C SDK.
Unlike the C SDK setting, `ServerDir` is the final watched PBD directory (for
example `/var/run/pfsd/vdb`), not its parent.

Build WAL-G without cgo using:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -tags pfsnative -o wal-g-pg-pfsnative ./main/pg
```

`WALG_PFSD_TIMEOUT` controls mount and implicit file read/write timeouts. I/O
is split into requests of at most 1 MiB for compatibility with the validated
PFSD variants.

PFSD permits only one live process for a PBD/host-ID pair. A PostgreSQL
`archive_command` process therefore needs a host ID different from a
simultaneous `backup-push`. For restore, prefer `wal-g daemon` plus
`walg-daemon-client`: PostgreSQL routinely probes missing WAL/history files,
and a long-lived daemon keeps one mount across those expected failures.

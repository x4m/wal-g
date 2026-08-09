# Native Go PFSD transport prototype

This package is an experimental, cgo-free implementation of the PFSD shared
memory transport. It currently implements the connection lifecycle and a
read-only filesystem slice (`Stat`, `Open`, and sequential `Read`):

1. create and lock the SDK pidfile;
2. send the mount request using the PFSD v2 binary layout;
3. wait for and validate the daemon acknowledgement;
4. map and validate all seven shared-memory regions;
5. signal unmount, close the pidfile, and wait for daemon cleanup.

It is not wired into WAL-G. The request path uses atomic request ownership,
epochs, cancellation via `REQ_ZOMBIE`, and response-state polling. PFSD's own
SDK documents the response semaphore as a CPU-usage optimization; completion
is published through the atomic `REQ_WAIT_RELEASE` state, so the native client
does not call into glibc to wait. Calls are currently serialized per client.

Before this can become a storage implementation it still needs directory
operations and all mutating operations. Writes need special care around daemon
restart and ambiguous completion; they are intentionally outside this first
prototype.

The prototype is intentionally restricted to Linux/amd64. The pidfile protocol
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

Use a host ID reserved for the test. This prototype does not yet acquire the
additional `/var/run/pfs/<pbd>-paxos-hostid` range lock used by the C SDK.
Unlike the C SDK setting, `ServerDir` is the final watched PBD directory (for
example `/var/run/pfsd/vdb`), not its parent.

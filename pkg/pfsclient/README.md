# pfsclient

`pfsclient` is a reusable Go client for PolarDB File System. It talks directly
to `pfsdaemon` through the upstream C++ SDK and does not depend on WAL-G storage
interfaces or require a FUSE mount.

The package is available on Linux when the `pfs` build tag and CGO are enabled.
Install PolarDB-FileSystem under `/usr/local/polarstore/pfsd`, then build with:

```console
CGO_ENABLED=1 go build -tags pfs ./...
```

Example:

```go
client, err := pfsclient.Open(pfsclient.Config{
    Device: "nvme1n1",
    HostID: 1,
})
if err != nil {
    return err
}
defer client.Close()

file, err := client.OpenFile("/nvme1n1/data/file", os.O_RDONLY, 0)
if err != nil {
    return err
}
defer file.Close()
```

Errors can be inspected with `errors.As` into `*pfsclient.Error`,
`pfsclient.IsTemporary`, `pfsclient.IsAmbiguous`, and
`pfsclient.RequiresProcessRestart`. The latter means retry must happen in a
fresh process because the upstream C SDK may retain invalid global state after
a connection failure. An ambiguous mutating operation must be restarted at the
object/workflow level instead of repeated blindly.

The upstream SDK has process-global mount state. Multiple `Client` values may
share the same configuration; opening a client with a different configuration
while another client is active returns an error.

## Integration test

With `pfsdaemon` running and a writable directory already present on PFS:

```console
PFS_TEST_ROOT=/nvme1n1/wal-g-integration \
  go test -tags 'pfs integration' ./pkg/pfsclient -run TestClientRoundTrip
```

Optional connection overrides are `PFS_TEST_HOST_ID`, `PFS_TEST_CLUSTER`, and
`PFS_TEST_SERVER`. The test creates an isolated directory and exercises mkdir,
open, write, close, rename, read, stat, directory listing, and removal.

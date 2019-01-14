# WAL-G
[![Build Status](https://travis-ci.org/x4m/wal-g.svg?branch=master)](https://travis-ci.org/wal-g/wal-g)
[![Go Report Card](https://goreportcard.com/badge/github.com/x4m/wal-g)](https://goreportcard.com/report/github.com/x4m/wal-g)

WAL-G is an archival restoration tool for Postgres.

WAL-G is the successor of WAL-E with a number of key differences. WAL-G uses LZ4, LZMA or Brotli compression, multiple processors and non-exclusive base backups for Postgres. More information on the design and implementation of WAL-G can be found on the Citus Data blog post ["Introducing WAL-G by Citus: Faster Disaster Recovery for Postgres"](https://www.citusdata.com/blog/2017/08/18/introducing-wal-g-faster-restores-for-postgres/).

**Table of Contents**
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Development](#development)
	- [Installing](#installing)
	- [Testing](#testing)
- [Authors](#authors)
- [License](#license)
- [Acknowledgements](#acknowledgements)

Installation
----------
A precompiled binary for Linux AMD 64 of the latest version of WAL-G can be obtained under the [Releases tab](https://github.com/x4m/wal-g/releases).

To decompress the binary, use:

```
tar -zxvf wal-g.linux-amd64.tar.gz
```
For other incompatible systems, please consult the Development section for more information.

Configuration
-------------
**Required**

WAL-G uses [the usual PostgreSQL environment variables](https://www.postgresql.org/docs/current/static/libpq-envars.html) to configure its connection, especially including `PGHOST`, `PGPORT`, `PGUSER`, and `PGPASSWORD`/`PGPASSFILE`/`~/.pgpass`.


To connect to Amazon S3, WAL-G requires that this variable be set:

* `WALG_S3_PREFIX` (eg. `s3://bucket/path/to/folder`) (alternative form `WALE_S3_PREFIX`)

WAL-G determines AWS credentials [like other AWS tools](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#config-settings-and-precedence). You can set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (optionally with `AWS_SECURITY_TOKEN`), or `~/.aws/credentials` (optionally with `AWS_PROFILE`), or you can set nothing to automatically fetch credentials from the EC2 metadata service.


To store backups in Google Cloud Storage, WAL-G requires that this variable be set:

* `WALG_GS_PREFIX` to specify where to store backups (eg. `gs://x4m-test-bucket/walg-folder`) 

WAL-G determines Google Cloud credentials using [application-default credentials](https://cloud.google.com/docs/authentication/production) like other GCP tools. You can set `GOOGLE_APPLICATION_CREDENTIALS` to point to a service account json key from GCP. If you set nothing, WAL-G will attempt to fetch credentials from the GCE/GKE metadata service.


To store backups on files system, WAL-G requires that these variables be set:

* `WALG_FILE_PREFIX` (eg. `/tmp/wal-g-test-data`)

Please, keep in mind that by default storing backups on disk along with database is not safe. Do not use it as a disaster recovery plan.


**Optional**

`PGHOST` can connect over a UNIX socket. This mode is preferred for localhost connections, set `PGHOST=/var/run/postgresql` to use it. WAL-G will connect over TCP if `PGHOST` is an IP address.

WAL-G can automatically determine the S3 bucket's region using `s3:GetBucketLocation`, but if you wish to avoid this API call or forbid it from the applicable IAM policy, specify:

* `AWS_REGION`(eg. `us-west-2`)

Concurrency values can be configured using:

* `WALG_DOWNLOAD_CONCURRENCY`

To configure how many goroutines to use during backup-fetch  and wal-push, use `WALG_DOWNLOAD_CONCURRENCY`. By default, WAL-G uses the minimum of the number of files to extract and 10.

* `WALG_UPLOAD_CONCURRENCY`

To configure how many concurrency streams to use during backup uploading, use `WALG_UPLOAD_CONCURRENCY`. By default, WAL-G uses 10 streams.

* `WALG_UPLOAD_DISK_CONCURRENCY`

To configure how many concurrency streams are reading disk during ```backup-push```. By default, WAL-G uses 1 stream.

* `WALG_SENTINEL_USER_DATA`

This setting allows backup automation tools to add extra information to JSON sentinel file during ```backup-push```. This setting can be used e.g. to give user-defined names to backups.

* `WALG_PREVENT_WAL_OVERWRITE`

If this setting is specified, during ```wal-push``` WAL-G will check the existence of WAL before uploading it. If the different file is already archived under the same name, WAL-G will return the non-zero exit code to prevent PostgreSQL from removing WAL.

* `AWS_ENDPOINT`

Overrides the default hostname to connect to an S3-compatible service. i.e, `http://s3-like-service:9000`

* `AWS_S3_FORCE_PATH_STYLE`

To enable path-style addressing(i.e., `http://s3.amazonaws.com/BUCKET/KEY`) when connecting to an S3-compatible service that lack of support for sub-domain style bucket URLs (i.e., `http://BUCKET.s3.amazonaws.com/KEY`). Defaults to `false`.

***Example: Using Minio.io S3-compatible storage***

```
AWS_ACCESS_KEY_ID: "<minio-key>"
AWS_SECRET_ACCESS_KEY: "<minio-secret>"
WALE_S3_PREFIX: "s3://my-minio-bucket/sub-dir"
AWS_ENDPOINT: "http://minio:9000"
AWS_S3_FORCE_PATH_STYLE: "true"
AWS_REGION: us-east-1
```

* `WALG_S3_STORAGE_CLASS`

To configure the S3 storage class used for backup files, use `WALG_S3_STORAGE_CLASS`. By default, WAL-G uses the "STANDARD" storage class. Other supported values include "STANDARD_IA" for Infrequent Access and "REDUCED_REDUNDANCY" for Reduced Redundancy.

* `WALG_S3_SSE`

To enable S3 server-side encryption, set to the algorithm to use when storing the objects in S3 (i.e., `AES256`, `aws:kms`).

* `WALG_S3_SSE_KMS_ID`

If using S3 server-side encryption with `aws:kms`, the KMS Key ID to use for object encryption.

* `WALG_GPG_KEY_ID`  (alternative form `WALE_GPG_KEY_ID`)

To configure GPG key for encryption and decryption. By default, no encryption is used. Public keyring is cached in the file "/.walg_key_cache".

* `WALG_DELTA_MAX_STEPS`

 Delta-backup is difference between previously taken backup and present state. `WALG_DELTA_MAX_STEPS` determines how many delta backups can be between full backups. Defaults to 0.
 Restoration process will automatically fetch all necessary deltas and base backup and compose valid restored backup (you still need WALs after start of last backup to restore consistent cluster).
 Delta computation is based on ModTime of file system and LSN number of pages in datafiles.

* `WALG_DELTA_ORIGIN`

 To configure base for next delta backup (only if `WALG_DELTA_MAX_STEPS` is not exceeded). `WALG_DELTA_ORIGIN` can be LATEST (chaining increments), LATEST_FULL (for bases where volatile part is compact and chaining has no meaning - deltas overwrite each other). Defaults to LATEST.

* `WALG_COMPRESSION_METHOD`

 To configure compression method used for backups. Possible options are: `lz4`, 'lzma'. Default method is `lz4`. LZ4 is the fastest method, but compression ratio is bad.
 LZMA is way much slower, however it compresses backups about 6 times better than LZ4. Brotli is a good trade-off between speed and compression ratio which is about 3 times better than LZ4.

 * `WALG_DISK_RATE_LIMIT`

  To configure disk read rate limit during ```backup-push``` in bytes per second.

 * `WALG_NETWORK_RATE_LIMIT`

  To configure network upload rate limit during ```backup-push``` in bytes per second.

Usage
-----

WAL-G currently supports these commands:


* ``backup-fetch``

When fetching base backups, the user should pass in the name of the backup and a path to a directory to extract to. If this directory does not exist, WAL-G will create it and any dependent subdirectories.

```
wal-g backup-fetch ~/extract/to/here example-backup
```

WAL-G can also fetch the latest backup using:

```
wal-g backup-fetch ~/extract/to/here LATEST
```

* ``backup-push``

When uploading backups to S3, the user should pass in the path containing the backup started by Postgres as in:

```
wal-g backup-push /backup/directory/path
```
If backup is pushed from replication slave, WAL-G will control timeline of the server. In case of promotion to master or timeline switch, backup will be uploaded but not finalized, WAL-G will exit with an error. In this case logs will contain information necessary to finalize the backup. You can use backuped data if you clearly understand entangled risks.

* ``wal-fetch``

When fetching WAL archives from S3, the user should pass in the archive name and the name of the file to download to. This file should not exist as WAL-G will create it for you.

WAL-G will also prefetch WAL files ahead of asked WAL file. These files will be cached in `./.wal-g/prefetch` directory. Cache files older than recently asked WAL file will be deleted from the cache, to prevent cache bloat. If the file is requested with `wal-fetch` this will also remove it from cache, but trigger fulfilment of cache with new file.

```
wal-g wal-fetch example-archive new-file-name
```

* ``wal-push``

When uploading WAL archives to S3, the user should pass in the absolute path to where the archive is located.

```
wal-g wal-push /path/to/archive
```

* ``backup-list``

Lists names and creation time of available backups.

* ``delete``

Is used to delete backups and WALs before them. By default ``delete`` will perform dry run. If you want to execute deletion you have to add ``--confirm`` flag at the end of the command.

``delete`` can operate in two modes: ``retain`` and ``before``.

``retain`` [FULL|FIND_FULL] %number%

if FULL is specified keep 5 full backups and everything in the middle

``before`` [FIND_FULL] %name%

if FIND_FULL is specified WAL-G will calculate minimum backup needed to keep all deltas alive. If FIND_FULL is not specified and call can produce orphaned deltas - call will fail with the list.

``retain 5`` will fail if 5th is delta

``retain FULL 5`` will keep 5 full backups and all deltas of them

``retain FIND_FULL`` will find necessary full for 5th

``before base_000010000123123123`` will fail if base_000010000123123123 is delta

``before FIND_FULL base_000010000123123123`` will keep everything after base of base_000010000123123123


Development
-----------
### Installing

To compile and build the binary:

```
go get github.com/x4m/wal-g
make deps
make all
```
Users can also install WAL-G by using `make install`. Specifying the GOBIN environment variable before installing allows the user to specify the installation location. On default, `make install` puts the compiled binary in `go/bin`.

```
export GOBIN=/usr/local/bin
make deps
make install
```

### Testing

WAL-G relies heavily on unit tests. These tests do not require S3 configuration as the upload/download parts are tested using mocked objects. For more information on testing, please consult [test_tools](test_tools).

WAL-G will perform a round-trip compression/decompression test that generates a directory for data (eg. data...), compressed files (eg. compressed), and extracted files (eg. extracted). These directories will only get cleaned up if the files in the original data directory match the files in the extracted one.

Test coverage can be obtained using:

```
go test -v -coverprofile=coverage.out
go tool cover -html=coverage.out
```


Authors
-------

* [Katie Li](https://github.com/katie31)
* [Daniel Farina](https://github.com/fdr)

See also the list of [contributors](CONTRIBUTORS) who participated in this project.

License
-------
This project is licensed under the Apache License, Version 2.0, but the lzo support is licensed under GPL 3.0+. Please refer to the [LICENSE.md](LICENSE.md) file for more details.

Acknowledgements
----------------
WAL-G would not have happened without the support of [Citus Data](https://www.citusdata.com/)

WAL-G came into existence as a result of the collaboration between a summer engineering intern at Citus, Katie Li, and Daniel Farina, the original author of WAL-E who currently serves as a principal engineer on the Citus Cloud team. Citus Data also has an [open source extension to Postgres](https://github.com/citusdata) that distributes database queries horizontally to deliver scale and performance.

Chat
----
We have a [Slack group](https://postgresteam.slack.com/messages/CA25P48P2) to discuss WAL-G usage and development. To joint PostgreSQL slack use [invite app](https://postgres-slack.herokuapp.com).

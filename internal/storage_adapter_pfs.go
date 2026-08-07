//go:build pfs && linux && cgo

package internal

import "github.com/wal-g/wal-g/pkg/storages/pfs"

var optionalStorageAdapters = []StorageAdapter{
	{"PFS", pfs.SettingList, pfs.ConfigureStorage},
}

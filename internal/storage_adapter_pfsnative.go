//go:build pfsnative && linux && amd64

package internal

import "github.com/wal-g/wal-g/pkg/storages/pfsnative"

var optionalStorageAdapters = []StorageAdapter{
	{"PFS", pfsnative.SettingList, pfsnative.ConfigureStorage},
}

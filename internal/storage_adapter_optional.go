//go:build (!pfs || !linux || !cgo) && (!pfsnative || !linux || !amd64)

package internal

var optionalStorageAdapters []StorageAdapter

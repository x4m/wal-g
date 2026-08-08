//go:build pfs && linux && cgo

package pfsclient

/*
#cgo CXXFLAGS: -std=c++11
#cgo CFLAGS: -I/usr/local/polarstore/pfsd/include
#cgo LDFLAGS: -L/usr/local/polarstore/pfsd/lib -lpfsd -lstdc++ -lrt -lpthread -ldl
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>
#include <pfsd_sdk.h>

static int64_t walg_pfs_stat_size(struct stat *st) { return st->st_size; }
static int64_t walg_pfs_stat_mtime(struct stat *st) { return st->st_mtime; }
static int walg_pfs_stat_is_dir(struct stat *st) { return S_ISDIR(st->st_mode); }
static const char *walg_pfs_dir_name(struct dirent *de) { return de->d_name; }
*/
import "C"

import (
	"time"
	"unsafe"
)

type fileInfo struct {
	size  int64
	mtime time.Time
	isDir bool
}

func sdkError(operation string, err error) error {
	return classifyError(operation, err)
}

func sdkSetServer(address string) {
	c := C.CString(address)
	defer C.free(unsafe.Pointer(c))
	C.pfsd_set_svr_addr(c, C.size_t(len(address)))
}

func sdkMount(cluster, pbd string, hostID, timeoutMS int) error {
	C.pfsd_set_mode(C.PFSD_SDK_THREADS)
	C.pfsd_set_connect_timeout(C.int(timeoutMS))
	cc := C.CString(cluster)
	defer C.free(unsafe.Pointer(cc))
	cp := C.CString(pbd)
	defer C.free(unsafe.Pointer(cp))
	if result, err := C.pfsd_mount(cc, cp, C.int(hostID), C.int(C.PFS_RDWR)); result != 0 {
		return sdkError("mount", err)
	}
	return nil
}

func sdkUnmount(pbd string) error {
	c := C.CString(pbd)
	defer C.free(unsafe.Pointer(c))
	if result, err := C.pfsd_umount(c); result != 0 {
		return sdkError("unmount", err)
	}
	return nil
}

func sdkStat(path string) (fileInfo, error) {
	c := C.CString(path)
	defer C.free(unsafe.Pointer(c))
	var st C.struct_stat
	if result, err := C.pfsd_stat(c, &st); result != 0 {
		return fileInfo{}, sdkError("stat", err)
	}
	return fileInfo{int64(C.walg_pfs_stat_size(&st)), time.Unix(int64(C.walg_pfs_stat_mtime(&st)), 0), C.walg_pfs_stat_is_dir(&st) != 0}, nil
}

func sdkMkdir(path string) error {
	c := C.CString(path)
	defer C.free(unsafe.Pointer(c))
	if result, err := C.pfsd_mkdir(c, 0755); result != 0 {
		return sdkError("mkdir", err)
	}
	return nil
}

func sdkUnlink(path string) error {
	c := C.CString(path)
	defer C.free(unsafe.Pointer(c))
	if result, err := C.pfsd_unlink(c); result != 0 {
		return sdkError("unlink", err)
	}
	return nil
}

func sdkRmdir(path string) error {
	c := C.CString(path)
	defer C.free(unsafe.Pointer(c))
	if result, err := C.pfsd_rmdir(c); result != 0 {
		return sdkError("rmdir", err)
	}
	return nil
}

func sdkRename(oldPath, newPath string) error {
	co := C.CString(oldPath)
	defer C.free(unsafe.Pointer(co))
	cn := C.CString(newPath)
	defer C.free(unsafe.Pointer(cn))
	if result, err := C.pfsd_rename(co, cn); result != 0 {
		return sdkError("rename", err)
	}
	return nil
}

func sdkOpen(path string, flags int, mode uint32) (int, error) {
	c := C.CString(path)
	defer C.free(unsafe.Pointer(c))
	fd, callErr := C.pfsd_open(c, C.int(flags), C.mode_t(mode))
	if fd < 0 {
		return 0, sdkError("open", callErr)
	}
	return int(fd), nil
}

func sdkClose(fd int) error {
	if result, err := C.pfsd_close(C.int(fd)); result != 0 {
		return sdkError("close", err)
	}
	return nil
}

func sdkRead(fd int, buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	n, callErr := C.pfsd_read(C.int(fd), unsafe.Pointer(&buffer[0]), C.size_t(len(buffer)))
	if n < 0 {
		return 0, sdkError("read", callErr)
	}
	return int(n), nil
}

func sdkWrite(fd int, buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	n, callErr := C.pfsd_write(C.int(fd), unsafe.Pointer(&buffer[0]), C.size_t(len(buffer)))
	if n < 0 {
		return 0, sdkError("write", callErr)
	}
	return int(n), nil
}

func sdkReadDir(path string) ([]string, error) {
	c := C.CString(path)
	defer C.free(unsafe.Pointer(c))
	dir, callErr := C.pfsd_opendir(c)
	if dir == nil {
		return nil, sdkError("opendir", callErr)
	}
	defer C.pfsd_closedir(dir)
	var names []string
	for {
		de := C.pfsd_readdir(dir)
		if de == nil {
			break
		}
		name := C.GoString(C.walg_pfs_dir_name(de))
		if name != "." && name != ".." {
			names = append(names, name)
		}
	}
	return names, nil
}

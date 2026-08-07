//go:build pfs && linux && cgo

package pfs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	clusterSetting = "WALG_PFS_CLUSTER"
	hostIDSetting  = "WALG_PFS_HOST_ID"
	serverSetting  = "WALG_PFSD_SERVER_ADDR"
	timeoutSetting = "WALG_PFSD_TIMEOUT"
	defaultCluster = "polarstore"
	defaultHostID  = 1
	defaultTimeout = 5 * time.Second
	prefixURL      = "pfs://"
)

var SettingList = []string{clusterSetting, hostIDSetting, serverSetting, timeoutSetting}

func ConfigureStorage(
	_ context.Context,
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	rootPath, pbdName, err := parsePrefix(prefix)
	if err != nil {
		return nil, err
	}

	hostID := defaultHostID
	if value := settings[hostIDSetting]; value != "" {
		hostID, err = strconv.Atoi(value)
		if err != nil || hostID < 0 {
			return nil, fmt.Errorf("parse %s: expected a non-negative integer, got %q", hostIDSetting, value)
		}
	}
	timeout := defaultTimeout
	if value := settings[timeoutSetting]; value != "" {
		timeout, err = time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return nil, fmt.Errorf("parse %s: expected a positive duration, got %q", timeoutSetting, value)
		}
	}
	cluster := settings[clusterSetting]
	if cluster == "" {
		cluster = defaultCluster
	}

	return newStorage(config{
		RootPath:  rootPath,
		PBDName:   pbdName,
		Cluster:   cluster,
		HostID:    hostID,
		Server:    settings[serverSetting],
		TimeoutMS: int(timeout / time.Millisecond),
	}, rootWraps...)
}

func parsePrefix(prefix string) (rootPath, pbdName string, err error) {
	prefix = strings.TrimPrefix(prefix, prefixURL)
	prefix = "/" + strings.TrimLeft(prefix, "/")
	parts := strings.Split(strings.TrimPrefix(prefix, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return "", "", fmt.Errorf("invalid PFS prefix %q: expected /<device>/<path>", prefix)
	}
	return prefix, parts[0], nil
}

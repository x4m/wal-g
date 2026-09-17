package postgres

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
)

func TestStopBackupArchiveWaitConfiguration(t *testing.T) {
	require.True(t, conf.PGAllowedSettings[conf.PgStopBackupWaitForArchive])
	for _, tc := range []struct {
		name, config, env string
		noWait, bad       bool
	}{
		{name: "default", config: `{}`},
		{name: "json false", config: `{"WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE": false}`, noWait: true},
		{name: "json true", config: `{"WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE": true}`},
		{name: "env false", config: `{}`, env: "false", noWait: true},
		{name: "env overrides file", config: `{"WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE": false}`, env: "true"},
		{name: "invalid env", config: `{}`, env: "typo", bad: true},
		{name: "invalid file", config: `{"WALG_STOP_BACKUP_WAIT_FOR_ARCHIVE": "typo"}`, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.AutomaticEnv()
			t.Cleanup(func() { require.NoError(t, viper.ReadConfig(strings.NewReader(`{}`))) })
			t.Setenv(conf.PgStopBackupWaitForArchive, tc.env)
			viper.SetConfigType("json")
			require.NoError(t, viper.ReadConfig(strings.NewReader(tc.config)))
			r := &PgQueryRunner{}
			err := r.configureStopBackupArchiveWait()
			if tc.bad {
				require.ErrorContains(t, err, conf.PgStopBackupWaitForArchive)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.noWait, r.stopBackupNoWait)
		})
	}
}

func TestStopBackupArchiveWaitVersions(t *testing.T) {
	for _, tc := range []struct {
		version      int
		wait, noWait string
	}{
		{90600, "pg_stop_backup(false)", "pg_stop_backup(false, false)"},
		{140000, "pg_stop_backup(false)", "pg_stop_backup(false, false)"},
		{150000, "pg_backup_stop()", "pg_backup_stop(false)"},
		{170000, "pg_backup_stop()", "pg_backup_stop(false)"},
	} {
		r := &PgQueryRunner{Version: tc.version}
		for _, wait := range []bool{true, false, true} {
			r.SetStopBackupArchiveWait(wait)
			q, err := r.BuildStopBackup()
			require.NoError(t, err)
			want := tc.noWait
			if wait {
				want = tc.wait
			}
			require.Equal(t, "SELECT labelfile, spcmapfile, lsn FROM pg_catalog."+want, q)
		}
	}
	r := &PgQueryRunner{Version: 90500}
	r.SetStopBackupArchiveWait(false)
	_, err := r.BuildStopBackup()
	require.ErrorContains(t, err, "9.6")
}

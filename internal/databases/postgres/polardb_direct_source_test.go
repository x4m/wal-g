package postgres

import (
	"testing"

	"github.com/spf13/viper"
	conf "github.com/wal-g/wal-g/internal/config"
)

func TestPolarDBWALDestination(t *testing.T) {
	testCases := []struct {
		root, walFileName, expected string
	}{
		{"/vdb/polar", "000000010000000000000001", "/vdb/polar/pg_wal/000000010000000000000001"},
		{"vdb/polar/", "pg_wal/RECOVERYXLOG", "/vdb/polar/pg_wal/RECOVERYXLOG"},
		{"/vdb/polar", "/local/pg_wal/00000002.history", "/vdb/polar/pg_wal/00000002.history"},
	}
	for _, testCase := range testCases {
		if actual := polarDBWALDestination(testCase.root, testCase.walFileName); actual != testCase.expected {
			t.Errorf("polarDBWALDestination(%q, %q) = %q, want %q",
				testCase.root, testCase.walFileName, actual, testCase.expected)
		}
	}
}

func TestIsPolarDBDirectPath(t *testing.T) {
	t.Setenv(PolarDBDirectDataPathEnv, "/vdb/polar")
	testCases := []struct {
		name string
		want bool
	}{
		{"/vdb/polar", true},
		{"/vdb/polar/pg_wal/000000010000000000000001", true},
		{"/vdb/polar-other/pg_wal/000000010000000000000001", false},
		{"/local/pg_wal/000000010000000000000001", false},
	}
	for _, testCase := range testCases {
		if got := isPolarDBDirectPath(testCase.name); got != testCase.want {
			t.Errorf("isPolarDBDirectPath(%q) = %t, want %t", testCase.name, got, testCase.want)
		}
	}
}

func TestPolarDBDirectDataPathFromViperConfig(t *testing.T) {
	if !conf.PGAllowedSettings[conf.PolarDBPFSDataPath] {
		t.Fatalf("%s is not registered as a PostgreSQL setting", conf.PolarDBPFSDataPath)
	}
	viper.Set(PolarDBDirectDataPathEnv, "/vdb/from-config")
	t.Cleanup(func() { viper.Set(PolarDBDirectDataPathEnv, nil) })

	if got := polarDBDirectDataPath(); got != "/vdb/from-config" {
		t.Fatalf("polarDBDirectDataPath() = %q, want config-file value", got)
	}
}

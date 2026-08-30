package postgres

import "testing"

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

package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/x4m/wal-g"
	"github.com/x4m/wal-g/walparser"
	"testing"
)

func TestExtractBlockLocations(t *testing.T) {
	record, _ := GetXLogRecordData()
	expectedLocations := []walparser.BlockLocation{record.Blocks[0].Header.BlockLocation}
	actualLocations := walg.ExtractBlockLocations([]walparser.XLogRecord{record})
	assert.Equal(t, expectedLocations, actualLocations)
}

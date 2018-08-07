package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"testing"
)

func TestDeleteArgsParsingRetain(t *testing.T) {
	var args walg.DeleteCommandArguments
	command := []string{"delete", "retain", "5"}

	assert.Falsef(t, parseAndTestFail(command, &args), "Parsing of delete comand failed")
	assert.Truef(t, args.Retain, "Parsing was wrong")
	assert.Falsef(t, args.FindFull, "Parsing was wrong")
	assert.Equalf(t, "5", args.Target, "Parsing was wrong")

	command = []string{"delete", "retain", "FULL", "5"}

	assert.Falsef(t, parseAndTestFail(command, &args), "Parsing of delete comand failed")
	assert.Truef(t, args.Retain, "Parsing was wrong")
	assert.Truef(t, args.Full, "Parsing was wrong")
	assert.Equalf(t, "5", args.Target, "Parsing was wrong")

	command = []string{"delete", "retain", "FIND_FULL", "5"}

	assert.Falsef(t, parseAndTestFail(command, &args), "Parsing of delete comand failed")
	assert.Truef(t, args.Retain, "Parsing was wrong")
	assert.Falsef(t, args.Full, "Parsing was wrong")
	assert.Equalf(t, "5", args.Target, "Parsing was wrong")
	assert.Truef(t, args.FindFull, "Parsing was wrong")

	command = []string{"delete", "re123tain", "FULL", "5"}

	assert.Truef(t, parseAndTestFail(command, &args), "Parsing of delete comand parsed wrong input")
}

func TestDeleteArgsParsingBefore(t *testing.T) {
	var args walg.DeleteCommandArguments
	command := []string{"delete", "before", "x"}

	assert.Falsef(t, parseAndTestFail(command, &args), "Parsing of delete comand failed")
	assert.Truef(t, args.Before, "Parsing was wrong")
	assert.Falsef(t, args.FindFull, "Parsing was wrong")
	assert.Equalf(t, "x", args.Target, "Parsing was wrong")
	assert.Falsef(t, args.Retain, "Parsing was wrong")

	command = []string{"delete", "before", "FIND_FULL", "x"}

	assert.Falsef(t, parseAndTestFail(command, &args), "Parsing of delete comand failed")
	assert.Truef(t, args.Before, "Parsing was wrong")
	assert.Truef(t, args.FindFull, "Parsing was wrong")
	assert.Equalf(t, "x", args.Target, "Parsing was wrong")
	assert.Nilf(t, args.BeforeTime, "Parsing was wrong")

	command = []string{"delete", "before", "FIND_FULL", "2014-11-12T11:45:26.371Z"}

	assert.Falsef(t, parseAndTestFail(command, &args), "Parsing of delete comand failed")
	assert.Truef(t, args.Before, "Parsing was wrong")
	assert.Truef(t, args.FindFull, "Parsing was wrong")
	assert.NotNilf(t, args.BeforeTime, "Parsing was wrong")

	command = []string{"delete"}

	assert.Truef(t, parseAndTestFail(command, &args), "Parsing of delete comand parsed wrong input")
}

func parseAndTestFail(command []string, arguments *walg.DeleteCommandArguments) bool {
	var failed bool
	result := walg.ParseDeleteArguments(command, func() { failed = true })
	*arguments = result
	return failed
}

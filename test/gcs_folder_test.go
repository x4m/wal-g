package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/x4m/wal-g/internal"
)

func TestGSFolder(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	storageFolder, err := internal.ConfigureGSFolder("gs://x4m-test/walg-bucket")

	assert.NoError(t, err)

	testStorageFolder(storageFolder, t)
}

package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
)

func TestBaseBackupSQL(t *testing.T) {
	options := pglogrepl.BaseBackupOptions{
		Label:             "wal'g",
		Fast:              true,
		MaxRate:           64,
		TablespaceMap:     true,
		NoVerifyChecksums: true,
	}

	assert.Equal(t,
		"BASE_BACKUP(LABEL 'wal''g', CHECKPOINT 'fast', MAX_RATE 64, TABLESPACE_MAP, VERIFY_CHECKSUMS false)",
		baseBackupSQL(options, 17))
	assert.Equal(t,
		"BASE_BACKUP LABEL 'wal''g' FAST MAX_RATE 64 TABLESPACE_MAP NOVERIFY_CHECKSUMS",
		baseBackupSQL(options, 14))
}

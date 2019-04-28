package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

const BackupServeShortDescription = "Serves a backup from storage for pg_basebackup"

// backupServeCmd represents the backupServe command
var backupServeCmd = &cobra.Command{
	Use:   "backup-serve destination_directory backup_name",
	Short: BackupServeShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		internal.HandleBackupServe(folder, args[0], args[1])
	},
}

var proxyCmd = &cobra.Command{
	Use:   "proxy",
	Short: "Postgres Protocol Proxy", // TODO : improve description
	Run: func(cmd *cobra.Command, args []string) {
		internal.HandleProxy()
	},
}

func init() {
	PgCmd.AddCommand(backupServeCmd)
	PgCmd.AddCommand(proxyCmd)
}

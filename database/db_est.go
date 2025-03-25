package database

import (
	"context"

	"github.com/dapplink-labs/multichain-sync-sol/config"
)

func SetupDb() *DB {
	dbConfig := config.NewTestDbConfig()

	newDB, _ := NewDB(context.Background(), *dbConfig)
	return newDB
}

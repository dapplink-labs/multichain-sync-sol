package database

import (
	"errors"
	"math/big"

	"gorm.io/gorm"

	"github.com/dapplink-labs/multichain-sync-sol/rpcclient"
)

type ReorgBlocks struct {
	Hash       string   `gorm:"primaryKey;"`
	ParentHash string   `json:"parent_hash"`
	Number     *big.Int `gorm:"serializer:u256"`
	Timestamp  uint64
}

type ReorgBlocksView interface {
	LatestReorgBlocks() (*rpcclient.BlockHeader, error)
}

type ReorgBlocksDB interface {
	ReorgBlocksView

	StoreReorgBlocks([]ReorgBlocks) error
}

type reorgBlocksDB struct {
	gorm *gorm.DB
}

func NewReorgBlocksDB(db *gorm.DB) ReorgBlocksDB {
	return &reorgBlocksDB{gorm: db}
}

func (db *reorgBlocksDB) StoreReorgBlocks(headers []ReorgBlocks) error {
	result := db.gorm.CreateInBatches(&headers, len(headers))
	return result.Error
}

func (db *reorgBlocksDB) LatestReorgBlocks() (*rpcclient.BlockHeader, error) {
	var header ReorgBlocks
	result := db.gorm.Order("number DESC").Take(&header)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return (*rpcclient.BlockHeader)(&header), nil
}

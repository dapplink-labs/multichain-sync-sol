package database

import (
	"errors"
	"gorm.io/gorm"
	"math/big"

	"github.com/google/uuid"
)

type Tokens struct {
	GUID          uuid.UUID `gorm:"primaryKey" json:"guid"`
	TokenAddress  string    `json:"token_address"`
	Decimals      uint8     `json:"uint"`
	TokenName     string    `json:"tokens_name"`
	CollectAmount *big.Int  `gorm:"serializer:u256" json:"collect_amount"`
	ColdAmount    *big.Int  `gorm:"serializer:u256" json:"cold_amount"`
	Timestamp     uint64    `json:"timestamp"`
}

type TokensView interface {
	TokensInfoByAddress(string, string) (*Tokens, error)
}

type TokensDB interface {
	TokensView

	StoreTokens(string, []Tokens) error
}

type tokensDB struct {
	gorm *gorm.DB
}

func NewTokensDB(db *gorm.DB) TokensDB {
	return &tokensDB{gorm: db}
}

func (db *tokensDB) StoreTokens(requestId string, tokenList []Tokens) error {
	result := db.gorm.Table("tokens_"+requestId).CreateInBatches(&tokenList, len(tokenList))
	return result.Error
}

func (db *tokensDB) TokensInfoByAddress(requestId string, address string) (*Tokens, error) {
	var tokensEntry Tokens
	err := db.gorm.Table("tokens_"+requestId).Where("token_address", address).Take(&tokensEntry).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &tokensEntry, nil
}

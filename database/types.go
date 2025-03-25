package database

import (
	"math/big"
)

type TokenBalance struct {
	FromAddress  string          `json:"from_address"`
	ToAddress    string          `json:"to_address"`
	TokenAddress string          `json:"to_ken_address"`
	Balance      *big.Int        `json:"balance"`
	TxType       TransactionType `json:"tx_type"`
}

package rpcclient

import (
	"math/big"
)

type BlockHeader struct {
	Hash       string
	ParentHash string
	Number     *big.Int
	Timestamp  uint64
}

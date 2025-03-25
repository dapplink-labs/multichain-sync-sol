package database

import (
	"errors"
	"fmt"
	"github.com/dapplink-labs/multichain-sync-sol/common/bigint"
	"gorm.io/gorm"
	"math/big"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"
)

type Internals struct {
	GUID                 uuid.UUID       `gorm:"primaryKey" json:"guid"`
	Timestamp            uint64          `json:"timestamp"`
	Status               TxStatus        `json:"status" gorm:"column:status"`
	BlockHash            string          `gorm:"column:block_hash" json:"block_hash"`
	BlockNumber          *big.Int        `gorm:"serializer:u256;column:block_number" json:"block_number"`
	TxHash               string          `gorm:"column:hash" json:"hash"`
	TxType               TransactionType `json:"tx_type" gorm:"column:tx_type"`
	FromAddress          string          `json:"from_address"`
	ToAddress            string          `json:"to_address"`
	Amount               *big.Int        `gorm:"serializer:u256;column:amount" json:"amount"`
	GasLimit             uint64          `json:"gas_limit"`
	MaxFeePerGas         string          `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas string          `json:"max_priority_fee_per_gas"`
	TokenType            TokenType       `json:"token_type" gorm:"column:token_type"` // ETH, ERC20, ERC721, ERC1155
	TokenAddress         string          `json:"token_address"`
	TokenId              string          `json:"token_id" gorm:"column:token_id"`     // ERC721/ERC1155 的 token ID
	TokenMeta            string          `json:"token_meta" gorm:"column:token_meta"` // Token 元数据
	TxSignHex            string          `json:"tx_sign_hex" gorm:"column:tx_sign_hex"`
}

type InternalsView interface {
	QueryNotifyInternal(requestId string) ([]*Internals, error)
	QueryInternalsByTxHash(requestId string, txHash string) (*Internals, error)
	QueryInternalsById(requestId string, guid string) (*Internals, error)
	UnSendInternalsList(requestId string) ([]*Internals, error)
}

type InternalsDB interface {
	InternalsView

	StoreInternal(string, *Internals) error
	UpdateInternalByTxHash(requestId string, txHash string, signedTx string, status TxStatus) error
	UpdateInternalById(requestId string, id string, signedTx string, status TxStatus) error
	UpdateInternalStatusByTxHash(requestId string, status TxStatus, internalsList []*Internals) error
	UpdateInternalListByHash(requestId string, internalsList []*Internals) error
	UpdateInternalListById(requestId string, internalsList []*Internals) error
	HandleFallBackInternals(requestId string, startBlock, EndBlock *big.Int) error
}

type internalsDB struct {
	gorm *gorm.DB
}

func NewInternalsDB(db *gorm.DB) InternalsDB {
	return &internalsDB{gorm: db}
}

func (db *internalsDB) QueryNotifyInternal(requestId string) ([]*Internals, error) {
	var notifyInternals []*Internals
	result := db.gorm.Table("internals_"+requestId).
		Where("status = ? or status = ?", TxStatusWalletDone, TxStatusNotified).
		Find(&notifyInternals)
	if result.Error != nil {
		return nil, result.Error
	}
	return notifyInternals, nil
}

func (db *internalsDB) StoreInternal(requestId string, internals *Internals) error {
	return db.gorm.Table("internals_" + requestId).Create(internals).Error
}

func (db *internalsDB) QueryInternalsByTxHash(requestId string, txHash string) (*Internals, error) {
	var internalsEntity Internals
	result := db.gorm.Table("internals_"+requestId).
		Where("hash = ?", txHash).
		Take(&internalsEntity)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &internalsEntity, nil
}

func (db *internalsDB) QueryInternalsById(requestId string, guid string) (*Internals, error) {
	var internalsEntity Internals
	result := db.gorm.Table("internals_"+requestId).
		Where("guid = ?", guid).
		Take(&internalsEntity)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &internalsEntity, nil
}

func (db *internalsDB) UnSendInternalsList(requestId string) ([]*Internals, error) {
	var internalsList []*Internals
	err := db.gorm.Table("internals_"+requestId).
		Where("status = ?", TxStatusSigned).
		Find(&internalsList).Error
	if err != nil {
		return nil, err
	}
	return internalsList, nil
}

type GasInfo struct {
	GasLimit             uint64
	MaxFeePerGas         string
	MaxPriorityFeePerGas string
}

func (db *internalsDB) UpdateInternalByTxHash(requestId string, txHash string, signedTx string, status TxStatus) error {
	updates := map[string]interface{}{
		"status": status,
	}

	if signedTx != "" {
		updates["tx_sign_hex"] = signedTx
	}

	result := db.gorm.Table("internals_"+requestId).
		Where("hash = ?", txHash).
		Updates(updates)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}

func (db *internalsDB) UpdateInternalById(requestId string, id string, signedTx string, status TxStatus) error {
	updates := map[string]interface{}{
		"status": status,
	}

	if signedTx != "" {
		updates["tx_sign_hex"] = signedTx
	}

	result := db.gorm.Table("internals_"+requestId).
		Where("guid = ?", id).
		Updates(updates)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}

func (db *internalsDB) UpdateInternalStatusByTxHash(requestId string, status TxStatus, internalsList []*Internals) error {
	if len(internalsList) == 0 {
		return nil
	}
	tableName := fmt.Sprintf("internals_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		var txHashList []string
		for _, internal := range internalsList {
			txHashList = append(txHashList, internal.TxHash)
		}

		result := tx.Table(tableName).
			Where("hash IN ?", txHashList).
			Update("status", status)

		if result.Error != nil {
			return fmt.Errorf("batch update status failed: %w", result.Error)
		}

		if result.RowsAffected == 0 {
			log.Warn("No internals updated",
				"requestId", requestId,
				"expectedCount", len(internalsList),
			)
		}

		log.Info("Batch update internals status success",
			"requestId", requestId,
			"count", result.RowsAffected,
			"status", status,
		)

		return nil
	})
}

func (db *internalsDB) UpdateInternalListById(requestId string, internalsList []*Internals) error {
	if len(internalsList) == 0 {
		return nil
	}
	tableName := fmt.Sprintf("internals_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, internal := range internalsList {
			// Update each record individually based on TxHash
			result := tx.Table(tableName).
				Where("guid = ?", internal.GUID).
				Updates(map[string]interface{}{
					"status": internal.Status,
					"amount": internal.Amount,
					"hash":   internal.TxHash,
				})

			// Check for errors in the update operation
			if result.Error != nil {
				return fmt.Errorf("update failed for TxHash %s: %w", internal.TxHash, result.Error)
			}

			// Log a warning if no rows were updated
			if result.RowsAffected == 0 {
				log.Info("No internals updated for TxHash:", "txHash", internal.TxHash)
			} else {
				// Log success message with the number of rows affected
				log.Info("Updated internals for ", "TxHash", internal.TxHash, "status", internal.Status, "amount", internal.Amount)
			}
		}

		return nil
	})
}

func (db *internalsDB) UpdateInternalListByHash(requestId string, internalsList []*Internals) error {
	if len(internalsList) == 0 {
		return nil
	}
	tableName := fmt.Sprintf("internals_%s", requestId)

	return db.gorm.Transaction(func(tx *gorm.DB) error {
		for _, internal := range internalsList {
			// Update each record individually based on TxHash
			result := tx.Table(tableName).
				Where("hash = ?", internal.TxHash).
				Updates(map[string]interface{}{
					"status": internal.Status,
					"amount": internal.Amount,
				})

			// Check for errors in the update operation
			if result.Error != nil {
				return fmt.Errorf("update failed for TxHash %s: %w", internal.TxHash, result.Error)
			}

			// Log a warning if no rows were updated
			if result.RowsAffected == 0 {
				log.Info("No internals updated for TxHash:", "txHash", internal.TxHash)
			} else {
				// Log success message with the number of rows affected
				log.Info("Updated internals for ", "TxHash", internal.TxHash, "status", internal.Status, "amount", internal.Amount)
			}
		}

		return nil
	})
}

func (db *internalsDB) HandleFallBackInternals(requestId string, startBlock, EndBlock *big.Int) error {
	for i := startBlock; i.Cmp(EndBlock) < 0; new(big.Int).Add(i, bigint.One) {
		var internalsSingle = Internals{}
		result := db.gorm.Table("internals_" + requestId).Where(&Transactions{BlockNumber: i}).Take(&internalsSingle)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}
		internalsSingle.Status = TxStatusFallback
		err := db.gorm.Table("internals_" + requestId).Save(&internalsSingle).Error
		if err != nil {
			return err
		}
	}
	return nil
}

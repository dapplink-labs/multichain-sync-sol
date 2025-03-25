package worker

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-sol/common/clock"
	"github.com/dapplink-labs/multichain-sync-sol/database"
	"github.com/dapplink-labs/multichain-sync-sol/rpcclient"
	"github.com/dapplink-labs/multichain-sync-sol/rpcclient/chain-account/account"
)

const TxHandleTaskBatchSize uint64 = 500

type Config struct {
	LoopIntervalMsec uint
	HeaderBufferSize uint
	StartHeight      *big.Int
	Confirmations    uint64
}

type BaseSynchronizer struct {
	loopInterval     time.Duration
	headerBufferSize uint64
	fromBlock        uint64

	rpcClient *rpcclient.WalletChainAccountClient
	database  *database.DB

	worker     *clock.LoopFn
	isFallBack bool

	batchWg *sync.WaitGroup
	bank    *ChannelBank
}

func (syncer *BaseSynchronizer) Start() error {
	if syncer.worker != nil {
		return errors.New("already started")
	}
	syncer.worker = clock.NewLoopFn(clock.SystemClock, syncer.tick, func() error {
		log.Info("shutting down batch producer")
		return nil
	}, syncer.loopInterval)
	return nil
}

func (syncer *BaseSynchronizer) Close() error {
	if syncer.worker == nil {
		return nil
	}
	return syncer.worker.Close()
}

// 启动的第一次需要取开始的区块
func (syncer *BaseSynchronizer) tick(ctx context.Context) {

	const batchSize = 500
	var batch []uint64
	for i := 0; i < 10; i += batchSize {
		syncer.batchWg.Add(1)
		go func(batch []uint64) {
			defer syncer.batchWg.Done()
			for _, h := range batch {
				block, err := syncer.BatchBlockHandle(0, 0)
				if err != nil {
					continue
				}
				log.Info("handle block", "h", h, "block", block)
			}
			// todo: 处理区块进入到不同的 channel
		}(batch)
	}
	go func() {
		syncer.batchWg.Wait()
		log.Info("all batches processed, closing channel")
		syncer.bank.Close()
	}()
}

func (syncer *BaseSynchronizer) BatchBlockHandle(startBlock uint64, endBlock uint64) ([]*account.BlockResponse, error) {
	var blocks []*account.BlockResponse
	for indexBlock := startBlock; indexBlock < endBlock; indexBlock++ {
		block, err := syncer.rpcClient.GetBlockInfo(indexBlock)
		if err != nil {
			log.Error("get block info fail", "err", err)
			return nil, err
		}
		blocks = append(blocks, block)
	}
	numBlocks := len(blocks)
	if numBlocks == 0 {
		return nil, nil
	}
	return blocks, nil
}

func (syncer *BaseSynchronizer) processHeader(blocks []*account.BlockResponse) (*database.Blocks, error) {
	return nil, nil
}

func (syncer *BaseSynchronizer) ScanBlockTransactions(txHashList []string, blockNumber *big.Int, blockHash string, bank *ChannelBank, wg *sync.WaitGroup) error {
	defer wg.Done()
	businessList, err := syncer.database.Business.QueryBusinessList()
	if err != nil {
		log.Error("query business list fail", "err", err)
		return err
	}
	for _, businessId := range businessList {
		for _, txHash := range txHashList {
			tx, err := syncer.rpcClient.GetTransactionByHash(txHash)
			if err != nil {
				return err
			}
			existToAddress, addressType := syncer.database.Addresses.AddressExist(businessId.BusinessUid, tx.To)
			existFromAddress, FromAddressType := syncer.database.Addresses.AddressExist(businessId.BusinessUid, tx.From)
			if !existToAddress && !existFromAddress {
				continue
			}
			log.Info("Found transaction", "txHash", tx.Hash, "from", tx.From, "to", tx.To)
			txItem := TransactionChannel{
				BusinessId:   businessId.BusinessUid,
				BlockNumber:  blockNumber,
				BlockHash:    blockHash,
				TxHash:       tx.Hash,
				FromAddress:  tx.From,
				ToAddress:    tx.To,
				Amount:       tx.Value,
				TxFee:        tx.Fee,
				TxStatus:     1,
				TokenAddress: tx.ContractAddress,
				TxType:       database.TxTypeUnKnow,
			}

			if !existFromAddress && (existToAddress && addressType == database.AddressTypeUser) { // 充值
				log.Info("Found deposit transaction", "txHash", tx.Hash, "from", tx.From, "to", tx.To)
				txItem.TxType = database.TxTypeDeposit
			}

			if (existFromAddress && FromAddressType == database.AddressTypeHot) && !existToAddress { // 提现
				log.Info("Found withdraw transaction", "txHash", tx.Hash, "from", tx.From, "to", tx.To)
				txItem.TxType = database.TxTypeWithdraw
			}

			if (existFromAddress && FromAddressType == database.AddressTypeUser) && (existToAddress && addressType == database.AddressTypeHot) { // 归集
				log.Info("Found collection transaction", "txHash", tx.Hash, "from", tx.From, "to", tx.To)
				txItem.TxType = database.TxTypeCollection
			}

			if (existFromAddress && FromAddressType == database.AddressTypeHot) && (existToAddress && addressType == database.AddressTypeCold) { // 热转冷
				log.Info("Found hot2cold transaction", "txHash", tx.Hash, "from", tx.From, "to", tx.To)
				txItem.TxType = database.TxTypeHot2Cold
			}

			if (existFromAddress && FromAddressType == database.AddressTypeCold) && (existToAddress && addressType == database.AddressTypeHot) { // 热转冷
				log.Info("Found cold2hot transaction", "txHash", tx.Hash, "from", tx.From, "to", tx.To)
				txItem.TxType = database.TxTypeCold2Hot
			}
			bank.Push(txItem)
			time.Sleep(1 * time.Millisecond)
		}
	}
	return nil
}

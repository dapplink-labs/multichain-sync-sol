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

	rpcClient  *rpcclient.WalletChainAccountClient
	blockBatch *rpcclient.BatchBlock
	database   *database.DB

	headers []rpcclient.BlockHeader
	worker  *clock.LoopFn

	fallbackBlockHeader *rpcclient.BlockHeader

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

func (syncer *BaseSynchronizer) tick(ctx context.Context) {
	const batchSize = 500
	newHeaders, fallBlockHeader, isReorg, err := syncer.blockBatch.NextHeaders(syncer.headerBufferSize)
	if err != nil {
		if isReorg && errors.Is(err, rpcclient.ErrBlockFallBack) {
			if !syncer.isFallBack {
				log.Warn("found block fallback, start fallback task")
				syncer.isFallBack = true
				syncer.fallbackBlockHeader = fallBlockHeader
			}
		} else {
			log.Error("error querying headers", "err", err)
		}
		return
	}
	if len(newHeaders) == 0 {
		log.Info("no new headers, already at chain head")
		return
	}
	for i := 0; i < len(newHeaders); i += batchSize {
		end := i + batchSize
		if end > len(newHeaders) {
			end = len(newHeaders)
		}
		batch := newHeaders[i:end]
		syncer.batchWg.Add(1)
		go func(batch []rpcclient.BlockHeader) {
			defer syncer.batchWg.Done()
			for _, h := range batch {
				block, err := syncer.processHeader(h)
				if err != nil {
					log.Error("scan block failed", "block", h.Number, "err", err)
					continue
				}
				log.Info("handle block", "blockNumber", block.Number)
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

func (syncer *BaseSynchronizer) processHeader(header rpcclient.BlockHeader) (*database.Blocks, error) {
	var txHashList []string
	txList, err := syncer.rpcClient.GetBlockInfo(header.Number)
	if err != nil {
		log.Error("get block info fail", "err", err)
		return nil, err
	}
	for _, txHash := range txList {
		txHashList = append(txHashList, txHash.Hash)
	}
	cb := NewChannelBank(1000)
	var wg sync.WaitGroup
	err = syncer.ScanBlockTransactions(txHashList, header.Number, header.Hash, cb, &wg)
	if err != nil {
		return nil, err
	}
	return &database.Blocks{
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
		Number:     header.Number,
		Timestamp:  header.Timestamp,
	}, nil
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

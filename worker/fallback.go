package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-sol/common/bigint"
	"github.com/dapplink-labs/multichain-sync-sol/common/retry"
	"github.com/dapplink-labs/multichain-sync-sol/common/tasks"
	"github.com/dapplink-labs/multichain-sync-sol/config"
	"github.com/dapplink-labs/multichain-sync-sol/database"
	"github.com/dapplink-labs/multichain-sync-sol/rpcclient"
)

type FallBack struct {
	deposit        *Deposit
	database       *database.DB
	rpcClient      *rpcclient.WalletChainAccountClient
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewFallBack(cfg *config.Config, db *database.DB, rpcClient *rpcclient.WalletChainAccountClient, deposit *Deposit, shutdown context.CancelCauseFunc) (*FallBack, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &FallBack{
		deposit:        deposit,
		database:       db,
		rpcClient:      rpcClient,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in fallback: %w", err))
		}},
		ticker: time.NewTicker(time.Second * 3),
	}, nil
}

func (fb *FallBack) Close() error {
	var result error
	fb.resourceCancel()
	fb.ticker.Stop()
	log.Info("stop fallback......")
	if err := fb.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await fallback %w", err))
		return result
	}
	log.Info("stop fallback success")
	return nil
}

func (fb *FallBack) Start() error {
	log.Info("start fallback......")
	fb.tasks.Go(func() error {
		for {
			select {
			case <-fb.ticker.C:
				log.Info("fallback task", "depositIsFallBack", fb.deposit.isFallBack)
				if fb.deposit.isFallBack {
					log.Info("notified of fallback", "fallbackBlockNumber", fb.deposit.fallbackBlockHeader.Number)
					if err := fb.onFallBack(fb.deposit.fallbackBlockHeader); err != nil {
						log.Error("handle fallback block fail", "err", err)
					}
					fb.deposit.isFallBack = false
					fb.deposit.fallbackBlockHeader = nil
				} else {
					log.Info("no block fallback, waiting for fallback task coming")
				}
			case <-fb.resourceCtx.Done():
				log.Info("stop fallback in worker")
				return nil
			}
		}
	})
	return nil
}

func (fb *FallBack) onFallBack(fallbackBlockHeader *rpcclient.BlockHeader) error {
	var reorgBlockHeader []database.ReorgBlocks
	var chainBlocks []database.Blocks
	lastBlockHeader := fallbackBlockHeader
	// 充值交易：把用户地址上的资金减掉
	// 提现交易：把热钱包地址上的资金加上
	// 归集(用户地址到热钱包)：把热钱包地址上的资金减掉，把用户地址上的资金加上
	// 热转温(用户地址到热钱包)：把热钱包地址上的资金加上，把温钱包地址上的资金减掉
	// 温转热(用户地址到热钱包)：把温钱包地址上的资金加上，把热钱包地址上的资金加上
	// var balances []*database.FbTokenAddressBalance
	// 文件 mock 数据，然后进行
	// - 先 mock 1000
	// - 让 900-1000 的 Hash 发生成
	for {
		lastBlockNumber := new(big.Int).Sub(lastBlockHeader.Number, bigint.One)

		log.Info("start get block header info", "lastBlockNumber", lastBlockNumber)

		chainBlockHeader, err := fb.rpcClient.GetBlockHeader(lastBlockNumber)
		if err != nil {
			log.Warn("query block from chain err", "err", err)
			break
		}

		dbBlockHeader, err := fb.database.Blocks.QueryBlocksByNumber(lastBlockNumber)
		if err != nil {
			log.Warn("query block from database err", "err", err)
			break
		}
		log.Info("query blocks success", "dbBlockHeaderHash", dbBlockHeader.Hash)
		chainBlocks = append(chainBlocks, database.Blocks{
			Hash:       dbBlockHeader.Hash,
			ParentHash: dbBlockHeader.ParentHash,
			Number:     dbBlockHeader.Number,
			Timestamp:  dbBlockHeader.Timestamp,
		})

		reorgBlockHeader = append(reorgBlockHeader, database.ReorgBlocks{
			Hash:       dbBlockHeader.Hash,
			ParentHash: dbBlockHeader.ParentHash,
			Number:     dbBlockHeader.Number,
			Timestamp:  dbBlockHeader.Timestamp,
		})

		if lastBlockHeader.ParentHash == dbBlockHeader.Hash {
			log.Info("lastBlockHeader chainBlockHeader ", "lastBlockParentHash", lastBlockHeader.ParentHash, "lastBlockNumber", lastBlockHeader.Number, "chainBlockHash", chainBlockHeader.Hash, "chainBlockHeaderNumber", chainBlockHeader.Number)
			break
		}
		lastBlockHeader = chainBlockHeader
	}

	businessList, err := fb.database.Business.QueryBusinessList()
	if err != nil {
		log.Error("Query business list fail", "err", err)
		return err
	}
	var fallbackBalances []*database.TokenBalance
	for _, businessItem := range businessList {
		log.Info("handle business", "BusinessUid", businessItem.BusinessUid)
		//transactionsList, err := fb.database.Transactions.QueryFallBackTransactions(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number)
		//if err != nil {
		//	return err
		//}
		//for _, transaction := range transactionsList {
		//	fbb := &database.TokenBalance{
		//		FromAddress:  transaction.FromAddress,
		//		ToAddress:    transaction.ToAddress,
		//		TokenAddress: transaction.TokenAddress,
		//		Balance:      new(big.Int).Neg(transaction.Amount),
		//		TxType:       transaction.TxType,
		//	}
		//	fallbackBalances = append(fallbackBalances, fbb)
		//}
	}

	retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
	if _, err := retry.Do[interface{}](fb.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
		if err := fb.database.Transaction(func(tx *database.DB) error {
			if len(reorgBlockHeader) > 0 {
				log.Info("Store reorg block success", "totalTx", len(reorgBlockHeader))
				if err := tx.ReorgBlocks.StoreReorgBlocks(reorgBlockHeader); err != nil {
					return err
				}
			}
			if len(chainBlocks) > 0 {
				log.Info("delete block success", "totalTx", len(reorgBlockHeader))
				if err := tx.Blocks.DeleteBlocksByNumber(chainBlocks); err != nil {
					return err
				}
			}
			if fallbackBlockHeader.Number.Cmp(lastBlockHeader.Number) > 0 {
				for _, businessItem := range businessList {
					if err := tx.Deposits.HandleFallBackDeposits(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Internals.HandleFallBackInternals(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Withdraws.HandleFallBackWithdraw(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Transactions.HandleFallBackTransactions(businessItem.BusinessUid, lastBlockHeader.Number, fallbackBlockHeader.Number); err != nil {
						return err
					}
					if err := tx.Balances.UpdateOrCreate(businessItem.BusinessUid, fallbackBalances); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			log.Error("unable to persist batch", "err", err)
			return nil, err
		}
		return nil, nil
	}); err != nil {
		return err
	}
	return nil
}

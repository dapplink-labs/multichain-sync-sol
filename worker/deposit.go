package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-sol/common/tasks"
	"github.com/dapplink-labs/multichain-sync-sol/config"
	"github.com/dapplink-labs/multichain-sync-sol/database"
	"github.com/dapplink-labs/multichain-sync-sol/rpcclient"
	"github.com/dapplink-labs/multichain-sync-sol/rpcclient/chain-account/account"
)

type Deposit struct {
	BaseSynchronizer
	confirms       uint8
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewDeposit(cfg *config.Config, db *database.DB, rpcClient *rpcclient.WalletChainAccountClient, shutdown context.CancelCauseFunc) (*Deposit, error) {
	var fromBlock uint64
	dbLatestBlockHeader, err := db.Blocks.LatestBlocks()
	if err != nil {
		log.Error("get latest block from database fail")
		return nil, err
	}
	if dbLatestBlockHeader != nil {
		log.Info("sync bock", "number", dbLatestBlockHeader.Number, "hash", dbLatestBlockHeader.Hash)
		fromBlock = dbLatestBlockHeader.Number.Uint64()
	} else if cfg.ChainNode.StartingHeight > 0 {
		chainLatestBlockHeader, err := rpcClient.GetBlockHeader(big.NewInt(int64(cfg.ChainNode.StartingHeight)))
		if err != nil {
			log.Error("get block from chain account fail", "err", err)
			return nil, err
		}
		fromBlock, _ = strconv.ParseUint(chainLatestBlockHeader.BlockHeader.Number, 10, 64)
	} else {
		chainLatestBlockHeader, err := rpcClient.GetBlockHeader(nil)
		if err != nil {
			log.Error("get block from chain account fail", "err", err)
			return nil, err
		}
		fromBlock, _ = strconv.ParseUint(chainLatestBlockHeader.BlockHeader.Number, 10, 64)
	}

	baseSyncer := BaseSynchronizer{
		fromBlock:        fromBlock,
		loopInterval:     cfg.ChainNode.SynchronizerInterval,
		headerBufferSize: cfg.ChainNode.BlocksStep,
		rpcClient:        rpcClient,
		database:         db,
		isFallBack:       false,
	}

	resCtx, resCancel := context.WithCancel(context.Background())

	return &Deposit{
		BaseSynchronizer: baseSyncer,
		confirms:         uint8(cfg.ChainNode.Confirmations),
		resourceCtx:      resCtx,
		resourceCancel:   resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in deposit: %w", err))
		}},
	}, nil
}

func (deposit *Deposit) Close() error {
	var result error
	if err := deposit.BaseSynchronizer.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to close internal base synchronizer: %w", err))
	}
	deposit.resourceCancel()
	if err := deposit.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await batch handler completion: %w", err))
	}
	return result
}

func (deposit *Deposit) Start() error {
	log.Info("starting deposit...")
	if err := deposit.BaseSynchronizer.Start(); err != nil {
		return fmt.Errorf("failed to start internal Synchronizer: %w", err)
	}

	deposit.tasks.Go(func() error {
		// todo: channel reader function
		return nil
	})
	return nil
}

func (deposit *Deposit) handleBatch(batch map[string]*TransactionChannel) error {
	return nil
}

func (deposit *Deposit) HandleDeposit(txMsg *account.TxMessage) (*database.Deposits, error) {
	return nil, nil
}

func (deposit *Deposit) HandleWithdraw(txMsg *account.TxMessage) (*database.Withdraws, error) {
	return nil, nil
}

func (deposit *Deposit) HandleInternalTx(txMsg *account.TxMessage) (*database.Internals, error) {
	return nil, nil
}

func (deposit *Deposit) BuildTransaction(txMsg *account.TxMessage) (*database.Transactions, error) {
	return nil, nil
}

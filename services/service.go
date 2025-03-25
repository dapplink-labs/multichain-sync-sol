package services

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ethereum/go-ethereum/log"

	"github.com/dapplink-labs/multichain-sync-sol/database"
	"github.com/dapplink-labs/multichain-sync-sol/protobuf/dal-wallet-go"
	"github.com/dapplink-labs/multichain-sync-sol/rpcclient"
)

const MaxRecvMessageSize = 1024 * 1024 * 300

type BusinessMiddleConfig struct {
	GrpcHostname string
	GrpcPort     int
}

type BusinessMiddleWireServices struct {
	*BusinessMiddleConfig
	accountClient *rpcclient.WalletChainAccountClient
	db            *database.DB
	stopped       atomic.Bool
}

func (bws *BusinessMiddleWireServices) Stop(ctx context.Context) error {
	bws.stopped.Store(true)
	return nil
}

func (bws *BusinessMiddleWireServices) Stopped() bool {
	return bws.stopped.Load()
}

func NewBusinessMiddleWireServices(db *database.DB, config *BusinessMiddleConfig, accountClient *rpcclient.WalletChainAccountClient) (*BusinessMiddleWireServices, error) {
	return &BusinessMiddleWireServices{
		BusinessMiddleConfig: config,
		accountClient:        accountClient,
		db:                   db,
	}, nil
}

func (bws *BusinessMiddleWireServices) Start(ctx context.Context) error {
	go func(bws *BusinessMiddleWireServices) {
		addr := fmt.Sprintf("%s:%d", bws.GrpcHostname, bws.GrpcPort)
		log.Info("start rpc server", "addr", addr)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error("Could not start tcp listener. ")
		}
		gs := grpc.NewServer(
			grpc.MaxRecvMsgSize(MaxRecvMessageSize),
			grpc.ChainUnaryInterceptor(
				nil,
			),
		)
		reflection.Register(gs)

		dal_wallet_go.RegisterBusinessMiddleWireServicesServer(gs, bws)

		log.Info("Grpc info", "port", bws.GrpcPort, "address", listener.Addr())
		if err := gs.Serve(listener); err != nil {
			log.Error("Could not GRPC server")
		}
	}(bws)
	return nil
}

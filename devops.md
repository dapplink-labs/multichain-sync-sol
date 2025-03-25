# 一键发发钱包的 local 部署方案

## 1.签名机

### 1.1. 克隆项目
```
git clone git@github.com:dapplink-labs/wallet-sign-go.git
```

### 1.2.项目构建
```
cd wallet-sign-go
make build
```

### 1.3.配置环境变量

```
export SIGNATURE_RPC_PORT=8983
export SIGNATURE_RPC_HOST="127.0.0.1"
export SIGNATURE_LEVEL_DB_PATH="./data"
```

### 1.4.环境变量生效
```
source .env
```

### 1.5.启动服务

- 命令行参数
```
guoshijiang@guoshijiangdeMacBook-Pro wallet-sign-go % ./signature
NAME:
   signature - A new cli application

USAGE:
   signature [global options] command [command options]

VERSION:
   1.14.11-stable-c9377cc0

DESCRIPTION:
   An exchange wallet scanner services with rpc and rest api services

COMMANDS:
   rpc
   version
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h     show help
   --version, -v  print the version
```

- 命令启动

```
./signature rpc
```

## 2.统一 RPC 服务 gateWay

### 1.1. 克隆项目
```
git clone git@github.com:dapplink-labs/wallet-chain-account.git
```

### 1.2.项目构建
```
cd wallet-chain-account
make build
```

### 1.3.配置 yaml

```
server:
  port: 8189

network: mainnet

chains: [Ethereum, Solana, Tron, Sui, Ton]

wallet_node:
  eth:
    rpc_url: 'https://eth-holesky.g.alchemy.com/v2/BvSZ5ZfdIwB-5SDXMz8PfGcbICYQqwrl'
    rpc_user: ''
    rpc_pass: ''
    data_api_url: 'https://api.etherscan.io/api?'
    data_api_key: 'HZEZGEPJJDA633N421AYW9NE8JFNZZC7JT'
    data_api_token: ''
    time_out: 15

  cosmos:
    rpc_url: 'https://cosmos-rpc.publicnode.com:443'
    rpc_user: ''
    rpc_pass: ''
    data_api_url: 'https://cosmos-rest.publicnode.com/'
    data_api_key: 'HZEZGEPJJDA633N421AYW9NE8JFNZZC7JT'
    data_api_token: ''
    time_out: 15

  solana:
    rpc_url: 'https://go.getblock.io/0b210f6491324f969c052746cce6c0dd'
    rpc_user: ''
    rpc_pass: ''
    data_api_url: 'https://public-api.solscan.io'
    data_api_key: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3MjQwNjIxMzk5MDYsImVtYWlsIjoiemFja2d1by5ndW9AZ21haWwuY29tIiwiYWN0aW9uIjoidG9rZW4tYXBpIiwiYXBpVmVyc2lvbiI6InYxIiwiaWF0IjoxNzI0MDYyMTM5fQ.EaWDC25lyGNx_LqRL5sAYYKLMbq10brnexKnAz9C3UY'
    data_api_token: ''
    time_out: 15

  tron:
    rpc_url: 'https://api.trongrid.io'
    rpc_user: 'TRON-PRO-API-KEY'
    rpc_pass: 'f7ae3e01-17d1-4a31-92b3-57f99457d915'
    data_api_url: 'https://www.oklink.com'
    data_api_key: '5181d535-b68f-41cf-bbc6-25905e46b6a6'
    data_api_token: ''
    time_out: 15

  sui:
    rpc_url: 'https://sui-mainnet-endpoint.blockvision.org'
    rpc_user: ''
    rpc_pass: ''
    data_api_url: ''
    data_api_key: ''
    data_api_token: ''
    time_out: 15

  ton:
    rpc_url: 'https://ton.org/global.config.json'
    rpc_user: ''
    rpc_pass: ''
    data_api_url: 'https://toncenter.com/api/v3'
    data_api_key: ''
    data_api_token: ''
    time_out: 15
```


### 1.5.启动服务

```
./wallet-chain-account -c ./config.yml
```


## 3.统一扫链业务平台

### 1.1. 克隆项目
```
git clone git@github.com:dapplink-labs/multichain-sync-account.git
```

### 1.2.项目构建
```
cd multichain-sync-account
make build
```

### 1.3.配置环境变量

```
export WALLET_MIGRATIONS_DIR=""./migrations""
export WALLET_CHAIN_ID="1"
export WALLET_CHAIN_NAME="Ethereum"
export WALLET_TRADING_MODEL="Ethereum"
export WALLET_RPC_RUL="127.0.0.1:8289"
export WALLET_STARTING_HEIGHT=2781450
export WALLET_CONFIRMATIONS=10
export WALLET_SYNC_INTERVAL=5s
export WALLET_WORKER_INTERVAL=5s
export WALLET_BLOCKS_STEP=2
export WALLET_RPC_HOST="127.0.0.1"
export WALLET_RPC_PORT=8987
export WALLET_CHAIN_ACCOUNT_RPC="127.0.0.1:8189"
export WALLET_METRICS_HOST="127.0.0.1"
export WALLET_METRICS_PORT=8986
export WALLET_SLAVE_DB_ENABLE=false
export WALLET_API_CACHE_ENABLE=false
export WALLET_MASTER_DB_HOST="127.0.0.1"
export WALLET_MASTER_DB_PORT=5432
export WALLET_MASTER_DB_USER="guoshijiang"
export WALLET_MASTER_DB_PASSWORD=""
export WALLET_MASTER_DB_NAME="multichain"
export WALLET_SLAVE_DB_HOST="127.0.0.1"
export WALLET_SLAVE_DB_PORT=5432
export WALLET_SLAVE_DB_USER="guoshijiang"
export WALLET_SLAVE_DB_PASSWORD=""
export WALLET_SLAVE_DB_NAME="multichain"
export WALLET_API_CACHE_LIST_SIZE=100000
export WALLET_API_CACHE_LIST_DETAIL=100000
export WALLET_API_CACHE_LIST_EXPIRE_TIME=10s
export WALLET_API_CACHE_DETAIL_EXPIRE_TIME=10s
```

```
source .env
```

### 1.5 数据库生成
```
./multichain-sync migrate
```

### 1.6.启动服务

- 启动 rpc 服务
```
./wallet-chain-account rpc 
```

- 启动扫链服务
```
./wallet-chain-account rpc 
```

- 启动通知服务
```
./wallet-chain-account notify 
```

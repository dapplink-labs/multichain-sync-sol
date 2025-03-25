## 1.创建未签名交易

- Request
```
grpcurl -plaintext -d '{
  "requestId": "dapplink",
  "chainId": "17000",
  "chain": "Ethereum",
  "from": "0xf3ee4862f9cb414c6e90a2474bccf2991e128036",
  "to": "0xfdfea0f029e50e7c3c3fc0e3b5487a64c0f3b918",
  "value": "150000000000000000",
  "contractAddress": "0x00",
  "txType": "collection"
}' 127.0.0.1:8987 syncs.BusinessMiddleWireServices.createUnSignTransaction
```

- Response

```
{
  "code": "SUCCESS",
  "msg": "submit withdraw and build un sign tranaction success",
  "transaction_id": "2e486a2b-a9d2-41b9-ada9-1ce1b8fac1fb",
  "un_sign_tx": "0x9ca77bd43a45da2399da96159b554bebdd89839eec73a8ff0626abfb2fb4b538"
}
```


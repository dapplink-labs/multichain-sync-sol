# notifier module


## 1.Function Overview

Fetch depsit, withdraw, collect, to cold transaction from database and then call fallback api submit transaction information to business platform

## 1.1.Deposit

获取未通知业务的交易通知业务层，已经过了确认为的交易，通知完业务层，直接将状体修改为已完成交易，若改交易还没有过确认，不需要修改状态，下一次继续通知业务层，以便于业务层知道目前交易的确认位情况

## 1.1.withdraw, collect, to cold transaction 

交易扫到落库之后，直接通知业务层，通知完成之后将交易状态改为已完成
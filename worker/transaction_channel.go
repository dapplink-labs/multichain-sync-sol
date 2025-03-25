package worker

import (
	"container/heap"
	"math/big"
	"sync"

	"github.com/dapplink-labs/multichain-sync-sol/database"
)

type TransactionChannel struct {
	BusinessId      string
	BlockNumber     *big.Int
	BlockHash       string
	TxHash          string
	FromAddress     string
	ToAddress       string
	TokenAddress    string
	Amount          string
	TxFee           string
	TxStatus        uint8
	TxType          database.TransactionType
	ContractAddress string
}

type txMinHeap []TransactionChannel

func (h txMinHeap) Len() int { return len(h) }
func (h txMinHeap) Less(i, j int) bool {
	return h[i].BlockNumber.Cmp(h[j].BlockNumber) < 0
}
func (h txMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *txMinHeap) Push(x interface{}) {
	*h = append(*h, x.(TransactionChannel))
}

func (h *txMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type ChannelBank struct {
	inputCh  chan TransactionChannel
	outputCh chan TransactionChannel
	buffer   txMinHeap
	mu       sync.Mutex
	cond     *sync.Cond
	closed   bool
}

// NewChannelBank 创建一个新的 ChannelBank 实例
func NewChannelBank(bufferSize int) *ChannelBank {
	cb := &ChannelBank{
		inputCh:  make(chan TransactionChannel, bufferSize),
		outputCh: make(chan TransactionChannel, bufferSize),
		buffer:   make(txMinHeap, 0),
	}
	cb.cond = sync.NewCond(&cb.mu)
	go cb.sorter()
	return cb
}

func (cb *ChannelBank) Push(tx TransactionChannel) {
	cb.inputCh <- tx
}

func (cb *ChannelBank) Channel() <-chan TransactionChannel {
	return cb.outputCh
}

func (cb *ChannelBank) Close() {
	cb.mu.Lock()
	cb.closed = true
	close(cb.inputCh)
	cb.cond.Broadcast()
	cb.mu.Unlock()
}

func (cb *ChannelBank) sorter() {
	for {
		cb.mu.Lock()
	LOOP:
		for {
			select {
			case tx, ok := <-cb.inputCh:
				if !ok {
					break LOOP
				}
				heap.Push(&cb.buffer, tx)
				cb.cond.Signal()
			default:
				break LOOP
			}
		}

		if len(cb.buffer) > 0 {
			tx := heap.Pop(&cb.buffer).(TransactionChannel)
			cb.mu.Unlock()
			cb.outputCh <- tx
		} else {
			if cb.closed {
				cb.mu.Unlock()
				break
			}
			cb.cond.Wait()
			cb.mu.Unlock()
		}
	}

	cb.mu.Lock()
	for len(cb.buffer) > 0 {
		tx := heap.Pop(&cb.buffer).(TransactionChannel)
		cb.outputCh <- tx
	}
	cb.mu.Unlock()
	close(cb.outputCh)
}

type BlockHeader struct {
	Number     *big.Int
	Hash       string
	ParentHash string
	Timestamp  uint64
}

type bhMinHeap []BlockHeader

func (h bhMinHeap) Len() int { return len(h) }
func (h bhMinHeap) Less(i, j int) bool {
	return h[i].Number.Cmp(h[j].Number) < 0
}
func (h bhMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *bhMinHeap) Push(x interface{}) {
	*h = append(*h, x.(BlockHeader))
}

func (h *bhMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type BlockHeaderBank struct {
	inputCh  chan BlockHeader
	outputCh chan BlockHeader
	buffer   bhMinHeap
	mu       sync.Mutex
	cond     *sync.Cond
	closed   bool
}

func NewBlockHeaderBank(bufferSize int) *BlockHeaderBank {
	bhb := &BlockHeaderBank{
		inputCh:  make(chan BlockHeader, bufferSize),
		outputCh: make(chan BlockHeader, bufferSize),
		buffer:   make(bhMinHeap, 0),
	}
	bhb.cond = sync.NewCond(&bhb.mu)
	go bhb.sorter()
	return bhb
}

func (bhb *BlockHeaderBank) Push(bh BlockHeader) {
	bhb.inputCh <- bh
}

func (bhb *BlockHeaderBank) Channel() <-chan BlockHeader {
	return bhb.outputCh
}

func (bhb *BlockHeaderBank) Close() {
	bhb.mu.Lock()
	bhb.closed = true
	close(bhb.inputCh)
	bhb.cond.Broadcast()
	bhb.mu.Unlock()
}

func (bhb *BlockHeaderBank) sorter() {
	for {
		bhb.mu.Lock()
	LOOP:
		for {
			select {
			case bh, ok := <-bhb.inputCh:
				if !ok {
					break LOOP
				}
				heap.Push(&bhb.buffer, bh)
				bhb.cond.Signal()
			default:
				break LOOP
			}
		}

		if len(bhb.buffer) > 0 {
			bh := heap.Pop(&bhb.buffer).(BlockHeader)
			bhb.mu.Unlock()
			bhb.outputCh <- bh
		} else {
			if bhb.closed {
				bhb.mu.Unlock()
				break
			}
			bhb.cond.Wait()
			bhb.mu.Unlock()
		}
	}

	bhb.mu.Lock()
	for len(bhb.buffer) > 0 {
		bh := heap.Pop(&bhb.buffer).(BlockHeader)
		bhb.outputCh <- bh
	}
	bhb.mu.Unlock()
	close(bhb.outputCh)
}

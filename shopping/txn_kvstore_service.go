package shopping

import (
	"distributed-system/twopc"
	"encoding/gob"
	"rush-shopping/kv"
	"sync"
)

// TransKVStore is the kvstore supported shopping transations.
type ShoppingTxnKVStore struct {
	*kv.KVStore
	transRwLock sync.RWMutex
}

func NewShoppingTxnKVStore() *ShoppingTxnKVStore {
	tks := &ShoppingTxnKVStore{KVStore: kv.NewKVStore()}
	return tks
}

type ShoppingTxnKVStoreService struct {
	*ShoppingTxnKVStore
	ppt     *twopc.Participant
	network string
}

// NewShoppingTxnKVStoreService starts the transaction-enabled kvstore.
func NewShoppingTxnKVStoreService(network, addr, coordAddr string) *ShoppingTxnKVStoreService {
	ppt := twopc.NewParticipant(network, addr, coordAddr)
	sks := NewShoppingTxnKVStore()

	gob.Register(AddItemTxnInitRet{})
	ppt.RegisterCaller(twopc.CallFunc(sks.CartExist), "CartExist")
	ppt.RegisterCaller(twopc.CallFunc(sks.CartAddItem), "CartAddItem")

	gob.Register(SubmitOrderTxnInitRet{})
	ppt.RegisterCaller(twopc.CallFunc(sks.ItemsStockMinus), "ItemsStockMinus")
	ppt.RegisterCaller(twopc.CallFunc(sks.OrderRecord), "OrderRecord")

	gob.Register(PayOrderTxnInitRet{})
	ppt.RegisterCaller(twopc.CallFunc(sks.PayMinus), "PayMinus")
	ppt.RegisterCaller(twopc.CallFunc(sks.PayAdd), "PayAdd")
	ppt.RegisterCaller(twopc.CallFunc(sks.PayRecord), "PayRecord")
	service := &ShoppingTxnKVStoreService{ShoppingTxnKVStore: sks, network: network, ppt: ppt}
	ppt.RegisterRPCService(service)
	return service
}

// Serve starts the KV-Store service.
func (service *ShoppingTxnKVStoreService) Serve() {
	// ?
}

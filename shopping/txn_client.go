package shopping

import (
	"distributed-system/twopc"
	"distributed-system/util"
	"net/rpc"
	"rush-shopping/kv"
)

type CoordClients struct {
	pool *util.ResourcePool
}

func NewCoordClients(network, addr string, size int) *CoordClients {
	pool := util.NewResourcePool(func() util.Resource {
		return util.DialServer(network, addr)
	}, size)
	return &CoordClients{pool: pool}
}

func (cs *CoordClients) SyncTxn(txnID string) (errCode int) {
	// return 0
	var reply twopc.TxnState
	c := cs.pool.Get().(*rpc.Client)
	defer cs.pool.Put(c)

	call := c.Go("Coordinator.SyncTxnEnd", &txnID, &reply, nil)
	<-call.Done

	errCode = reply.ErrCode
	return
}

func (cs *CoordClients) AsyncAddItemTxn(cartIDStr, userToken string, itemID,
	addItemCnt int) (ok bool, txnID string) {
	args := &AddItemArgs{CartIDStr: cartIDStr, UserToken: userToken,
		ItemID: itemID, AddItemCnt: addItemCnt}
	ok = util.RPCPoolCall(cs.pool, "ShoppingTxnCoordinator.AsyncAddItemTxn", args, &txnID)
	return
}

func (cs *CoordClients) AsyncSubmitOrderTxn(cartIDStr,
	userToken string) (ok bool, txnID string) {
	args := &SubmitOrderArgs{CartIDStr: cartIDStr, UserToken: userToken}
	ok = util.RPCPoolCall(cs.pool, "ShoppingTxnCoordinator.AsyncSubmitOrderTxn", args, &txnID)
	return
}

func (cs *CoordClients) AsyncPayOrderTxn(orderIDStr,
	userToken string, delta int) (ok bool, txnID string) {
	args := &PayOrderArgs{OrderIDStr: orderIDStr, UserToken: userToken, Delta: delta}
	ok = util.RPCPoolCall(cs.pool, "ShoppingTxnCoordinator.AsyncPayOrderTxn", args, &txnID)
	return
}

func (cs *CoordClients) LoadItemList(itemsCnt int) (ok bool) {
	ok = util.RPCPoolCall(cs.pool, "ShoppingTxnCoordinator.LoadItemList", &itemsCnt, &struct{}{})
	return
}

type ShardsClientHub struct {
	srvAddrs    []string
	pa          *util.ResourcePoolsArray
	keyHashFunc twopc.KeyHashFunc
	shards      int
}

func NewShardsClientHub(network string, srvAddrs []string,
	keyHashFunc twopc.KeyHashFunc, maxSizeForOne int) *ShardsClientHub {

	news := make([]func() util.Resource, len(srvAddrs))
	for i := 0; i < len(srvAddrs); i++ {
		addr := srvAddrs[i]
		news[i] = func() util.Resource {
			return util.DialServer(network, addr)
		}
	}
	pa := util.NewResourcePoolsArray(news,
		maxSizeForOne, len(srvAddrs))

	return &ShardsClientHub{srvAddrs: srvAddrs,
		pa: pa, keyHashFunc: keyHashFunc, shards: len(srvAddrs)}
}

func (h *ShardsClientHub) Put(key string, value string) (ok bool, reply kv.Reply) {

	args := &kv.PutArgs{Key: key, Value: value}
	ok = util.RPCPoolArrayCall(h.pa, int(h.keyHashFunc(key))%h.shards, "ShoppingTxnKVStoreService.RPCPut", args, &reply)
	return
}

func (h *ShardsClientHub) Get(key string) (ok bool, reply kv.Reply) {
	args := &kv.GetArgs{Key: key}
	ok = util.RPCPoolArrayCall(h.pa, int(h.keyHashFunc(key))%h.shards, "ShoppingTxnKVStoreService.RPCGet", args, &reply)
	return
}

func (h *ShardsClientHub) Incr(key string, delta int) (ok bool, reply kv.Reply) {
	args := &kv.IncrArgs{Key: key, Delta: delta}
	ok = util.RPCPoolArrayCall(h.pa, int(h.keyHashFunc(key))%h.shards, "ShoppingTxnKVStoreService.RPCIncr", args, &reply)
	return
}

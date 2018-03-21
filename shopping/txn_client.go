package shopping

import (
	"distributed-system/twopc"
	"fmt"
	"net"
	"net/rpc"
	"rush-shopping/kv"
	"syscall"
)

func DialServer(addr string) *rpc.Client {
	var err error
	rpcClient, err := rpc.Dial("tcp", addr)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("Dial(%v) failed: %v\n", addr, err1)
		}
		return nil
	}
	return rpcClient
}

type CoordClient struct {
	c *rpc.Client
}

func NewCoordClient(coordAddr string) *CoordClient {
	c := DialServer(coordAddr)
	if c == nil {
		return nil
	}
	return &CoordClient{c: c}
}
func (c *CoordClient) SyncTxn(txnID string) (errCode int) {
	var reply twopc.TxnState
	for {
		ok := call(c.c, "Coordinator.StateTxn", &txnID, &reply)
		if ok && (reply.State == twopc.StateTxnAborted || reply.State == twopc.StateTxnCommitted) {
			break
		}
	}
	errCode = reply.ErrCode
	return
}

func (c *CoordClient) StartAddItemTxn(cartIDStr, userToken string, itemID,
	addItemCnt int) (ok bool, txnID string) {
	args := &AddItemArgs{CartIDStr: cartIDStr, UserToken: userToken,
		ItemID: itemID, AddItemCnt: addItemCnt}
	ok = call(c.c, "ShoppingTxnCoordinator.StartAddItemTxn", args, &txnID)
	return
}

func (c *CoordClient) StartSubmitOrderTxn(cartIDStr,
	userToken string) (ok bool, txnID string) {
	args := &SubmitOrderArgs{CartIDStr: cartIDStr, UserToken: userToken}
	ok = call(c.c, "ShoppingTxnCoordinator.StartSubmitOrderTxn", args, &txnID)
	return
}

func (c *CoordClient) StartPayOrderTxn(orderIDStr,
	userToken string) (ok bool, txnID string) {
	args := &PayOrderArgs{OrderIDStr: orderIDStr, UserToken: userToken}
	ok = call(c.c, "ShoppingTxnCoordinator.StartPayOrderTxn", args, &txnID)
	return
}

func (c *CoordClient) LoadItemList(itemsCnt int) (ok bool) {
	ok = call(c.c, "ShoppingTxnCoordinator.LoadItemList", &itemsCnt, &struct{}{})
	return
}

func (c *CoordClient) Close() {
	c.c.Close()
}

type ShardsClient struct {
	SrvAddrs     []string
	shardClients []*rpc.Client
	keyHashFunc  twopc.KeyHashFunc
}

func NewShardsClient(srvAddrs []string,
	keyHashFunc twopc.KeyHashFunc) *ShardsClient {
	rpcClients := make([]*rpc.Client, len(srvAddrs))
	for i := 0; i < len(srvAddrs); i++ {
		rpcClients[i] = DialServer(srvAddrs[i])
		if rpcClients[i] == nil {
			return nil
		}
	}
	return &ShardsClient{SrvAddrs: srvAddrs,
		shardClients: rpcClients, keyHashFunc: keyHashFunc}
}

func (c *ShardsClient) Put(key string, value string) (ok bool, reply kv.Reply) {
	args := &kv.PutArgs{Key: key, Value: value}
	idx := int(c.keyHashFunc(key)) % len(c.shardClients)
	// fmt.Println(c.keyHashFunc(key), int(c.keyHashFunc(key)), "idx", idx)
	ok = call(c.shardClients[idx], "ShoppingTxnKVStoreService.RPCPut", args, &reply)
	return
}

func (c *ShardsClient) Get(key string) (ok bool, reply kv.Reply) {
	args := &kv.GetArgs{Key: key}
	ok = call(c.shardClients[int(c.keyHashFunc(key))%len(c.shardClients)], "ShoppingTxnKVStoreService.RPCGet", args, &reply)
	return
}

func (c *ShardsClient) Incr(key string, delta int) (ok bool, reply kv.Reply) {
	args := &kv.IncrArgs{Key: key, Delta: delta}
	ok = call(c.shardClients[int(c.keyHashFunc(key))%len(c.shardClients)], "ShoppingTxnKVStoreService.RPCIncr", args, &reply)
	return
}

func (c *ShardsClient) Close() {
	for _, client := range c.shardClients {
		client.Close()
	}
}

func call(client *rpc.Client, name string, args interface{}, reply interface{}) bool {
	err := client.Call(name, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

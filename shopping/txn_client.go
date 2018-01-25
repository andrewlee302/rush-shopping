package shopping

import (
	"fmt"
	"net"
	"net/rpc"
	"rush-shopping/kvstore"
	"syscall"
)

type TransKVClient struct {
	SrvAddr   string
	rpcClient *rpc.Client
}

func NewTransKVClient(srvAddr string) *TransKVClient {
	rpcClient, err := rpc.Dial("tcp", srvAddr)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("TransKVStore Dial() failed: %v\n", err1)
		}
		return nil
	}
	return &TransKVClient{SrvAddr: srvAddr, rpcClient: rpcClient}
}

func (c *TransKVClient) LoadItemList(itemsCnt int) (ok bool, reply int) {
	ok = c.call("TransKVStore.LoadItemList", &itemsCnt, &reply)
	return
}

func (c *TransKVClient) AddItemTrans(cartIDStr, userToken string, itemID,
	itemCnt int) (ok bool, reply int) {
	args := &AddItemArgs{CartIDStr: cartIDStr, UserToken: userToken,
		ItemID: itemID, ItemCnt: itemCnt}
	ok = c.call("TransKVStore.AddItemTrans", args, &reply)
	return
}

func (c *TransKVClient) SubmitOrderTrans(cartIDStr, userToken string) (ok bool, reply int) {
	args := &SubmitOrderArgs{CartIDStr: cartIDStr, UserToken: userToken}
	ok = c.call("TransKVStore.SubmitOrderTrans", args, &reply)
	return
}

func (c *TransKVClient) PayOrderTrans(orderIDStr, userToken string) (ok bool, reply int) {
	args := &PayOrderArgs{OrderIDStr: orderIDStr, UserToken: userToken}
	ok = c.call("TransKVStore.PayOrderTrans", args, &reply)
	return
}

func (c *TransKVClient) Close() {
	c.rpcClient.Close()
}

func (c *TransKVClient) Put(key string, value string) (ok bool, reply kvstore.Reply) {
	fmt.Println("here put")
	args := &kvstore.PutArgs{Key: key, Value: value}
	ok = c.call("TransKVStore.RPCPut", args, &reply)
	return
}

func (c *TransKVClient) Get(key string) (ok bool, reply kvstore.Reply) {
	args := &kvstore.GetArgs{Key: key}
	ok = c.call("TransKVStore.RPCGet", args, &reply)
	return
}

func (c *TransKVClient) Incr(key string, delta int) (ok bool, reply kvstore.Reply) {
	args := &kvstore.IncrArgs{Key: key, Delta: delta}
	ok = c.call("TransKVStore.RPCIncr", args, &reply)
	return
}

func (c *TransKVClient) call(name string, args interface{}, reply interface{}) bool {
	err := c.rpcClient.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

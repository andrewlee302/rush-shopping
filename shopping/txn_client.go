package shopping

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
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

func (c *TransKVClient) AddItemTrans(cartIDStr, userToken, itemIDStr string,
	itemCnt int) (ok bool, reply int) {
	args := &AddItemArgs{CartIDStr: cartIDStr, UserToken: userToken,
		ItemIDStr: itemIDStr, ItemCnt: itemCnt}
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

func (c *TransKVClient) HSet(key, field, value string) (ok bool, reply kvstore.Reply) {
	args := &kvstore.HSetArgs{Key: key, Field: field, Value: value}
	ok = c.call("TransKVStore.HSet", args, &reply)
	return
}

func (c *TransKVClient) HGet(key, field string) (ok bool, reply kvstore.Reply) {
	args := &kvstore.HGetArgs{Key: key, Field: field}
	ok = c.call("TransKVStore.HGet", args, &reply)
	return
}

func (c *TransKVClient) HGetAll(key string) (ok bool, reply kvstore.MapReply) {
	args := &kvstore.HGetAllArgs{Key: key}
	replyBinary := &kvstore.MapReplyBinary{}
	ok = c.call("TransKVStore.HGetAll", args, &replyBinary)
	reply.Flag = replyBinary.Flag
	if !reply.Flag {
		reply.Value = nil
	} else {
		buf := bytes.NewReader(replyBinary.Value)
		decoder := gob.NewDecoder(buf)
		if err := decoder.Decode(&reply.Value); err != nil {
			log.Fatal("decoder error:", err)
		}
	}
	return
}

func (c *TransKVClient) HIncr(key, field string, diff int) (ok bool, reply kvstore.Reply) {
	args := &kvstore.HIncrArgs{Key: key, Field: field, Diff: diff}
	ok = c.call("TransKVStore.HIncr", args, &reply)
	return
}

func (c *TransKVClient) HDel(key, field string) (ok bool) {
	args := &kvstore.HDelArgs{Key: key, Field: field}
	var reply kvstore.NoneStruct
	ok = c.call("TransKVStore.HDel", args, &reply)
	return
}

func (c *TransKVClient) HDelAll(key string) (ok bool) {
	args := &kvstore.HDelAllArgs{Key: key}
	var reply kvstore.NoneStruct
	ok = c.call("TransKVStore.HDelAll", args, &reply)
	return
}

func (c *TransKVClient) SAdd(key, member string) (ok bool, reply kvstore.Reply) {
	args := &kvstore.SAddArgs{Key: key, Member: member}
	ok = c.call("TransKVStore.SAdd", args, &reply)
	return
}

func (c *TransKVClient) SIsMember(key, member string) (ok bool, reply kvstore.Reply) {
	args := &kvstore.SIsMemberArgs{Key: key, Member: member}
	ok = c.call("TransKVStore.SIsMember", args, &reply)
	return
}

func (c *TransKVClient) SDel(key string) (ok bool) {
	args := &kvstore.SDelArgs{Key: key}
	var reply kvstore.NoneStruct
	ok = c.call("TransKVStore.SDel", args, &reply)
	return
}

func (c *TransKVClient) Put(key string, value string) (ok bool, reply kvstore.Reply) {
	fmt.Println("here put")
	args := &kvstore.PutArgs{Key: key, Value: value}
	ok = c.call("TransKVStore.Put", args, &reply)
	return
}

func (c *TransKVClient) Get(key string) (ok bool, reply kvstore.Reply) {
	args := &kvstore.GetArgs{Key: key}
	ok = c.call("TransKVStore.Get", args, &reply)
	return
}

func (c *TransKVClient) Del(key string) (ok bool) {
	args := &kvstore.DelArgs{Key: key}
	var reply kvstore.NoneStruct
	ok = c.call("TransKVStore.Del", args, &reply)
	return
}

func (c *TransKVClient) Incr(key string, diff int) (ok bool, reply kvstore.Reply) {
	args := &kvstore.IncrArgs{Key: key, Diff: diff}
	ok = c.call("TransKVStore.Incr", args, &reply)
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

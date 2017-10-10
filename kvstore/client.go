package kvstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"syscall"
)

type Client struct {
	SrvAddr   string
	rpcClient *rpc.Client
}

func NewClient(srvAddr string) *Client {
	rpcClient, err := rpc.Dial("tcp", srvAddr)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("TinyStore Dial() failed: %v\n", err1)
		}
		return nil
	}
	return &Client{SrvAddr: srvAddr, rpcClient: rpcClient}
}
func (c *Client) Close() {
	c.rpcClient.Close()
}

func (c *Client) HSet(key, field, value string) (ok bool, reply Reply) {
	args := &HSetArgs{Key: key, Field: field, Value: value}
	ok = c.call("TinyStore.HSet", args, &reply)
	return
}

func (c *Client) HGet(key, field string) (ok bool, reply Reply) {
	args := &HGetArgs{Key: key, Field: field}
	ok = c.call("TinyStore.HGet", args, &reply)
	return
}

func (c *Client) HGetAll(key string) (ok bool, reply MapReply) {
	args := &HGetAllArgs{Key: key}
	replyBinary := &MapReplyBinary{}
	ok = c.call("TinyStore.HGetAll", args, &replyBinary)
	buf := bytes.NewReader(replyBinary.Value)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&reply.Value); err != nil {
		log.Fatal("decoder error:", err)
	}
	reply.Flag = replyBinary.Flag
	return
}

func (c *Client) HIncr(key, field string, diff int) (ok bool, reply Reply) {
	args := &HIncrArgs{Key: key, Field: field, Diff: diff}
	ok = c.call("TinyStore.HIncr", args, &reply)
	return
}

func (c *Client) SAdd(key, member string) (ok bool, reply Reply) {
	args := &SAddArgs{Key: key, Member: member}
	ok = c.call("TinyStore.SAdd", args, &reply)
	return
}

func (c *Client) SIsMember(key, member string) (ok bool, reply Reply) {
	args := &SIsMemberArgs{Key: key, Member: member}
	ok = c.call("TinyStore.SIsMember", args, &reply)
	return
}

func (c *Client) Put(key string, value string) (ok bool, reply Reply) {
	args := &PutArgs{Key: key, Value: value}
	ok = c.call("TinyStore.Put", args, &reply)
	return
}

func (c *Client) Get(key string) (ok bool, reply Reply) {
	args := &GetArgs{Key: key}
	ok = c.call("TinyStore.Get", args, &reply)
	return
}

func (c *Client) Incr(key string, diff int) (ok bool, reply Reply) {
	args := &IncrArgs{Key: key, Diff: diff}
	ok = c.call("TinyStore.Incr", args, &reply)
	return
}

func (c *Client) CompareAndSet(key string, base, setValue int, compareOp func(int, int) bool) (ok bool, reply Reply) {
	args := &CompareAndSetArgs{Key: key, Base: base, SetValue: setValue, CompareOp: compareOp}
	ok = c.call("TinyStore.CompareAndSet", args, &reply)
	return
}

func (c *Client) CompareAndIncr(key string, base, diff int, compareOp func(int, int) bool) (ok bool, reply Reply) {
	args := &CompareAndIncrArgs{Key: key, Base: base, Diff: diff, CompareOp: compareOp}
	ok = c.call("TinyStore.CompareAndIncr", args, &reply)
	return
}

func (c *Client) call(name string, args interface{}, reply interface{}) bool {
	err := c.rpcClient.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

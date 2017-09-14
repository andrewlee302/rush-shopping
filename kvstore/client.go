package kvstore

import (
	"fmt"
	"net"
	"net/rpc"
	"syscall"
)

type Client struct {
	SrvAddr string
}

func NewClient(srvAddr string) *Client {
	return &Client{SrvAddr: srvAddr}
}

func (c *Client) Put(key string, value string) (reply Reply) {
	args := &PutArgs{Key: key, Value: value}
	c.call("TinyStore.Put", args, &reply)
	return
}

func (c *Client) Get(key string) (reply Reply) {
	args := &GetArgs{Key: key}
	c.call("TinyStore.Get", args, &reply)
	return
}

func (c *Client) Incr(key string, diff int) (reply Reply) {
	args := &IncrArgs{Key: key, Diff: diff}
	c.call("TinyStore.Incr", args, &reply)
	return
}

func (c *Client) CompareAndSet(key string, base, setValue int, compareOp func(int, int) bool) (reply Reply) {
	args := &CompareAndSetArgs{Key: key, Base: base, SetValue: setValue, CompareOp: compareOp}
	c.call("TinyStore.CompareAndSet", args, &reply)
	return
}

func (c *Client) CompareAndIncr(key string, base, diff int, compareOp func(int, int) bool) (reply Reply) {
	args := &CompareAndIncrArgs{Key: key, Base: base, Diff: diff, CompareOp: compareOp}
	c.call("TinyStore.CompareAndIncr", args, &reply)
	return
}

func (c *Client) call(name string, args interface{}, reply interface{}) bool {
	rpcClient, err := rpc.Dial("tcp", c.SrvAddr)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("TinyStore Dial() failed: %v\n", err1)
		}
		return false
	}
	defer rpcClient.Close()

	err = rpcClient.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

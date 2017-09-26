package kvstore

import (
	"runtime"
	"testing"
)

func checkMapCall(t *testing.T, ok bool, reply, expected MapReply) {
	if !ok {
		t.Fatalf("call error")
	}
	for k, v := range expected.Value {
		if v1, ok := reply.Value[k]; !ok || v1 != v {
			t.Fatalf("wrong reply <%v, %v>; expected <%v, %v>", k, v1, k, v)
			return
		}
	}
	for k, v := range reply.Value {
		if v1, ok := expected.Value[k]; !ok || v1 != v {
			t.Fatalf("wrong reply <%v, %v>; expected <%v, %v>", k, v, k, v1)
			return
		}
	}
}

func checkCall(t *testing.T, ok bool, reply, expected Reply) {
	if !ok {
		t.Fatalf("call error")
	} else if reply != expected {
		t.Fatalf("wrong reply %v; expected %v", reply, expected)
	}
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	srvAddr := "localhost:9090"
	StartTinyStore(srvAddr)
	client := NewClient(srvAddr)
	ok, reply := client.Get("key1")
	checkCall(t, ok, reply, Reply{Flag: false, Value: ""})

	ok, reply = client.Put("key1", "1")
	checkCall(t, ok, reply, Reply{Flag: false, Value: "1"})

	ok, reply = client.Get("key1")
	checkCall(t, ok, reply, Reply{Flag: true, Value: "1"})

	ok, reply = client.SIsMember("set1", "key1")
	checkCall(t, ok, reply, Reply{Flag: false, Value: ""})

	ok, reply = client.SAdd("set1", "key1")
	checkCall(t, ok, reply, Reply{Flag: false, Value: ""})

	ok, reply = client.SIsMember("set1", "key1")
	checkCall(t, ok, reply, Reply{Flag: true, Value: ""})

	ok, reply = client.SIsMember("set1", "key2")
	checkCall(t, ok, reply, Reply{Flag: false, Value: ""})

	ok, reply = client.HSet("hash1", "key1", "1")
	checkCall(t, ok, reply, Reply{Flag: false, Value: "1"})

	ok, reply = client.HGet("hash1", "key1")
	checkCall(t, ok, reply, Reply{Flag: true, Value: "1"})

	ok, reply = client.HSet("hash1", "key1", "2")
	checkCall(t, ok, reply, Reply{Flag: true, Value: "2"})

	ok, reply = client.HIncr("hash1", "key1", 1)
	checkCall(t, ok, reply, Reply{Flag: true, Value: "3"})

	ok, reply = client.HIncr("hash1", "key2", 10)
	checkCall(t, ok, reply, Reply{Flag: false, Value: "10"})

	ok, mapReply := client.HGetAll("hash1")
	checkMapCall(t, ok, mapReply, MapReply{Flag: true, Value: map[string]string{"key1": "3", "key2": "10"}})
}

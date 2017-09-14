package kvstore

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type TinyStore struct {
	dataset map[string]string
	l       net.Listener
	addr    string
}

func StartTinyStore(addr string) *TinyStore {
	ts := &TinyStore{dataset: make(map[string]string), addr: addr}
	rpcs := rpc.NewServer()
	rpcs.Register(ts)
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ts.l = l
	go func() {
		for {
			if conn, err := l.Accept(); err == nil {
				// concurrent processing
				go rpcs.ServeConn(conn)
			} else {
				log.Fatalln("!!!")
			}
		}
	}()
	return ts
}

// Put k-v pair, return whether it exists before.
// True if exists before, false otherwise.
func (ts *TinyStore) Put(args *PutArgs, reply *Reply) error {
	log.Println("put")
	_, ok := ts.dataset[args.Key]
	ts.dataset[args.Key] = args.Value
	reply.Flag, reply.Value = ok, args.Value
	return nil
}

// Return value of the specific key, "" if not exists.
func (ts *TinyStore) Get(args *GetArgs, reply *Reply) error {
	log.Println("get")
	reply.Value, reply.Flag = ts.dataset[args.Key]
	return nil
}

// Increase the value of the specific key, false if it doesn't
// exist or it's not a number.
// Increase can be negative.
func (ts *TinyStore) Incr(args *IncrArgs, reply *Reply) error {
	if value, ok := ts.dataset[args.Key]; ok {
		if iVal, err := strconv.Atoi(value); err != nil {
			newVal := strconv.Itoa(iVal + args.Diff)
			ts.dataset[args.Key] = newVal
			reply.Flag, reply.Value = true, newVal
		} else {
			reply.Flag, reply.Value = false, ""
		}
	} else {
		reply.Flag, reply.Value = false, ""
	}
	return nil
}

// If compareOp(Get(key), base) == true, then set value of the key to setVal
func (ts *TinyStore) CompareAndSet(args *CompareAndSetArgs, reply *Reply) error {
	if ok, _ := ts.compareValue(args.Key, args.Base, args.CompareOp); ok {
		newVal := strconv.Itoa(args.SetValue)
		ts.dataset[args.Key] = newVal
		reply.Flag, reply.Value = true, newVal
	} else {
		reply.Flag, reply.Value = false, ""
	}
	return nil
}

// If compareOp(Get(key), base) == true, then set value of the key to (Get(key) + diff)
func (ts *TinyStore) CompareAndIncr(args *CompareAndIncrArgs, reply *Reply) error {
	if ok, iVal := ts.compareValue(args.Key, args.Base, args.CompareOp); ok {
		newVal := strconv.Itoa(iVal + args.Diff)
		ts.dataset[args.Key] = newVal
		reply.Flag, reply.Value = true, newVal
	} else {
		reply.Flag, reply.Value = false, ""
	}
	return nil
}

// Compare the value of the specific key and return the result and the value.
// True iff value exists, it is a number and compareOp is true.
func (ts *TinyStore) compareValue(key string, base int, compareOp func(int, int) bool) (bool, int) {
	if value, ok := ts.dataset[key]; ok {
		if iVal, err := strconv.Atoi(value); err != nil && compareOp(iVal, base) {
			return true, iVal
		} else {
			return false, 0
		}
	} else {
		return false, 0
	}
}

package kvstore

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type TinyStore struct {
	flatdata map[string]string
	hashdata map[string](map[string]string)
	setdata  map[string](map[string]string)
	l        net.Listener
	addr     string
}

/**
 * Start the kvstore server and return immediately.
 */
func StartTinyStore(addr string) *TinyStore {
	log.Printf("Start kvstore service on %s\n", addr)
	defer log.Println("Closed kvstore service")
	ts := &TinyStore{flatdata: make(map[string]string), hashdata: make(map[string](map[string]string)), setdata: make(map[string](map[string]string)), addr: addr}
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

// Set the field of the specific hashmap.
func (ts *TinyStore) HSet(args *HSetArgs, reply *Reply) error {
	// log.Println("hset")
	hashmap, ok := ts.hashdata[args.Key]
	if !ok {
		hashmap = make(map[string]string)
		ts.hashdata[args.Key] = hashmap
	}
	_, ok = hashmap[args.Field]
	hashmap[args.Field] = args.Value
	reply.Flag, reply.Value = ok, args.Value
	return nil
}

// Get the field of the specific hashmap.
func (ts *TinyStore) HGet(args *HGetArgs, reply *Reply) error {
	// log.Println("hget")
	hashmap, ok := ts.hashdata[args.Key]
	if !ok {
		reply.Flag, reply.Value = false, ""
	} else {
		value, ok := hashmap[args.Field]
		if !ok {
			reply.Flag, reply.Value = false, ""
		} else {
			reply.Flag, reply.Value = true, value
		}
	}
	return nil
}

// Get all values of the specific hashmap.
func (ts *TinyStore) HGetAll(args *HGetAllArgs, reply *MapReply) error {
	log.Println("hgetall")
	hashmap, ok := ts.hashdata[args.Key]
	if !ok {
		reply.Flag, reply.Value = false, nil
	} else {
		reply.Flag, reply.Value = true, hashmap
	}
	return nil
}

// Increase the value of the field of the specific hashmap.
// If the field doesn't exist, then it equals the HSet operation.
func (ts *TinyStore) HIncr(args *HIncrArgs, reply *Reply) error {
	log.Println("hincr")
	if hashmap, ok := ts.hashdata[args.Key]; ok {
		if value, ok := hashmap[args.Field]; ok {
			if iVal, err := strconv.Atoi(value); err == nil {
				newVal := strconv.Itoa(iVal + args.Diff)
				hashmap[args.Field] = newVal
				reply.Flag, reply.Value = true, newVal
				return nil
			}
		}
	}
	value := strconv.Itoa(args.Diff)
	ts.HSet(&HSetArgs{Key: args.Key, Field: args.Field, Value: value}, reply)
	reply.Flag, reply.Value = false, value
	return nil
}

// Add the member into the set of the key.
func (ts *TinyStore) SAdd(args *SAddArgs, reply *Reply) error {
	log.Println("sadd")
	set, ok := ts.setdata[args.Key]
	if !ok {
		set = make(map[string]string)
		ts.setdata[args.Key] = set
	}
	_, ok = set[args.Member]
	if !ok {
		set[args.Member] = ""
	}
	reply.Flag, reply.Value = ok, ""
	return nil
}

// Test whether the member is actually in the set of the key.
func (ts *TinyStore) SIsMember(args *SIsMemberArgs, reply *Reply) error {
	log.Println("sismember")
	if set, ok := ts.setdata[args.Key]; !ok {
		reply.Flag, reply.Value = false, ""
	} else {
		if _, ok = set[args.Member]; !ok {
			reply.Flag, reply.Value = false, ""
		} else {
			reply.Flag, reply.Value = true, ""
		}
	}
	return nil
}

// Put k-v pair, return whether it exists before.
// True if exists before, false otherwise.
func (ts *TinyStore) Put(args *PutArgs, reply *Reply) error {
	// log.Println("put")
	_, ok := ts.flatdata[args.Key]
	ts.flatdata[args.Key] = args.Value
	reply.Flag, reply.Value = ok, args.Value
	return nil
}

// Return value of the specific key, "" if not exists.
func (ts *TinyStore) Get(args *GetArgs, reply *Reply) error {
	log.Println("get")
	reply.Value, reply.Flag = ts.flatdata[args.Key]
	return nil
}

// Increase the value of the specific key, false if it doesn't
// exist or it's not a number.
// Increase can be negative.
func (ts *TinyStore) Incr(args *IncrArgs, reply *Reply) error {
	log.Println("incr")
	if value, ok := ts.flatdata[args.Key]; ok {
		if iVal, err := strconv.Atoi(value); err == nil {
			newVal := strconv.Itoa(iVal + args.Diff)
			ts.flatdata[args.Key] = newVal
			reply.Flag, reply.Value = true, newVal
			return nil
		}
	}
	reply.Flag, reply.Value = false, ""
	return nil
}

// If compareOp(Get(key), base) == true, then set value of the key to setVal
func (ts *TinyStore) CompareAndSet(args *CompareAndSetArgs, reply *Reply) error {
	if ok, _ := ts.compareValue(args.Key, args.Base, args.CompareOp); ok {
		newVal := strconv.Itoa(args.SetValue)
		ts.flatdata[args.Key] = newVal
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
		ts.flatdata[args.Key] = newVal
		reply.Flag, reply.Value = true, newVal
	} else {
		reply.Flag, reply.Value = false, ""
	}
	return nil
}

// Compare the value of the specific key and return the result and the value.
// True iff value exists, it is a number and compareOp is true.
func (ts *TinyStore) compareValue(key string, base int, compareOp func(int, int) bool) (bool, int) {
	if value, ok := ts.flatdata[key]; ok {
		if iVal, err := strconv.Atoi(value); err == nil && compareOp(iVal, base) {
			return true, iVal
		} else {
			return false, 0
		}
	} else {
		return false, 0
	}
}

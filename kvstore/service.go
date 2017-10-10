package kvstore

import (
	"bytes"
	"encoding/gob"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
)

type TinyStore struct {
	flatdata map[string]string
	hashdata map[string](map[string]string)
	setdata  map[string](map[string]string)
	l        net.Listener
	addr     string
	rwLock   sync.RWMutex

	dead       int32 // for testing
	unreliable int32 // for testing

	// bytesPool *sync.Pool
}

/**
 * Start the kvstore server and return immediately.
 */
func StartTinyStore(addr string) *TinyStore {
	log.Printf("Start kvstore service on %s\n", addr)
	ts := &TinyStore{flatdata: make(map[string]string), hashdata: make(map[string](map[string]string)), setdata: make(map[string](map[string]string)), addr: addr}
	// bytesPool: &sync.Pool{
	// 	New: func() {
	// 		buf := make(byte, 1024)
	// 		return bytes.NewBuffer(buf)
	// 	},
	// },

	rpcs := rpc.NewServer()
	rpcs.Register(ts)
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ts.l = l
	go func() {
		for !ts.isDead() {
			if conn, err := l.Accept(); err == nil {
				if !ts.isDead() {
					// concurrent processing
					go rpcs.ServeConn(conn)
				} else if err == nil {
					conn.Close()
				}
			} else {
				if !ts.isDead() {
					log.Fatalln(err.Error())
				}
			}
		}
	}()
	return ts
}

func (ts *TinyStore) isDead() bool {
	return atomic.LoadInt32(&ts.dead) != 0
}

// Clear the data and close the kvstore service.
func (ts *TinyStore) Kill() {
	log.Println("Kill the kvstore")
	atomic.StoreInt32(&ts.dead, 1)
	ts.flatdata = nil
	ts.hashdata = nil
	ts.setdata = nil
	if err := ts.l.Close(); err != nil {
		log.Fatal("Kvsotre rPC server close error:", err)
	}
}

// Set the field of the specific hashmap.
// @Flag: true if the hashmap exists, false otherwise.
// @Value: new value.
func (ts *TinyStore) HSet(args *HSetArgs, reply *Reply) error {
	// log.Println("hset")
	ts.rwLock.Lock()
	defer ts.rwLock.Unlock()
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
// @Flag: true if the field exists, false otherwise.
// @Value: self if the field exists, "" otherwise.
func (ts *TinyStore) HGet(args *HGetArgs, reply *Reply) error {
	// log.Println("hget")
	ts.rwLock.RLock()
	defer ts.rwLock.RUnlock()
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
// @Flag: true if the hashmap exists, false otherwise.
// @Value: self if the hashmap exists, nil otherwise.
// NOTE: Take care of mutex of the shared structure returned by the function.
func (ts *TinyStore) HGetAll(args *HGetAllArgs, reply *MapReplyBinary) error {
	// log.Println("hgetall")
	ts.rwLock.RLock()
	defer ts.rwLock.RUnlock()
	hashmap, ok := ts.hashdata[args.Key]
	if !ok {
		reply.Flag, reply.Value = false, nil
	} else {
		writer := new(bytes.Buffer)
		encoder := gob.NewEncoder(writer)
		if err := encoder.Encode(hashmap); err != nil {
			log.Fatal("encode error:", err)
		}
		reply.Flag, reply.Value = true, writer.Bytes()
	}
	return nil
}

// Increase the value of the field of the specific hashmap.
// If the field doesn't exist, then it equals the HSet operation.
// @Flag: true if the field exists before, false otherwise.
// @Value: new value.
func (ts *TinyStore) HIncr(args *HIncrArgs, reply *Reply) error {
	// log.Println("hincr")
	ts.rwLock.Lock()
	defer ts.rwLock.Unlock()
	var hashmap map[string]string
	var ok bool
	if hashmap, ok = ts.hashdata[args.Key]; ok {
		if value, ok := hashmap[args.Field]; ok {
			if iVal, err := strconv.Atoi(value); err == nil {
				newVal := strconv.Itoa(iVal + args.Diff)
				hashmap[args.Field] = newVal
				reply.Flag, reply.Value = true, newVal
				return nil
			} else {
				reply.Flag, reply.Value = false, ""
				return nil
			}
		}
	} else {
		hashmap = make(map[string]string)
		ts.hashdata[args.Key] = hashmap
	}
	value := strconv.Itoa(args.Diff)
	hashmap[args.Field] = value
	reply.Flag, reply.Value = false, value
	return nil
}

// Add the member into the set of the key.
// @Flag: true if the set exists before, false otherwise.
// @Value: ""
func (ts *TinyStore) SAdd(args *SAddArgs, reply *Reply) error {
	// log.Println("sadd")
	ts.rwLock.Lock()
	defer ts.rwLock.Unlock()
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
// @Flag: true if the member exists, false otherwise.
// @Value: ""
func (ts *TinyStore) SIsMember(args *SIsMemberArgs, reply *Reply) error {
	// log.Println("sismember")
	ts.rwLock.RLock()
	defer ts.rwLock.RUnlock()
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

// Put k-v pair.
// @Flag: true if the key exists before, false otherwise.
// @Value: new value.
func (ts *TinyStore) Put(args *PutArgs, reply *Reply) error {
	// log.Println("put")
	ts.rwLock.Lock()
	defer ts.rwLock.Unlock()
	_, ok := ts.flatdata[args.Key]
	ts.flatdata[args.Key] = args.Value
	reply.Flag, reply.Value = ok, args.Value
	return nil
}

// Return value of the specific key.
// @Flag: true if the key exists, false otherwise.
// @Value: self if the key exists, "" otherwise.
func (ts *TinyStore) Get(args *GetArgs, reply *Reply) error {
	// log.Println("get")
	ts.rwLock.RLock()
	defer ts.rwLock.RUnlock()
	reply.Value, reply.Flag = ts.flatdata[args.Key]
	return nil
}

// Increase the value of the specific key and the increase can
// be negative.
// @Flag: true if the key exists before, false otherwise.
// @Value: new value.
func (ts *TinyStore) Incr(args *IncrArgs, reply *Reply) error {
	// log.Println("incr")
	ts.rwLock.Lock()
	defer ts.rwLock.Unlock()
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
// @Flag: true if the key exists, value is number and compareOp
// is true, false otherwise.
// @Value: new value if Flag is true, "" otherwise.
func (ts *TinyStore) CompareAndSet(args *CompareAndSetArgs, reply *Reply) error {
	ts.rwLock.Lock()
	defer ts.rwLock.Unlock()
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
// @Flag: true if the key exists, value is number and compareOp
// is true, false otherwise.
// @Value: new value if Flag is true, "" otherwise.
func (ts *TinyStore) CompareAndIncr(args *CompareAndIncrArgs, reply *Reply) error {
	ts.rwLock.Lock()
	defer ts.rwLock.Unlock()
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

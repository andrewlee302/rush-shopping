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

type TinyKVStore struct {
	flatdata map[string]string
	hashdata map[string](map[string]string)
	setdata  map[string](map[string]string)
	l        net.Listener
	addr     string

	// RwLock is the publi Read-Write Mutex, which can
	// be used in the extended KV-Store.
	RwLock sync.RWMutex

	dead       int32 // for testing
	unreliable int32 // for testing

	// bytesPool *sync.Pool

	// transId  int64
	// transMu  sync.RWMutex
	// transMap map[int64]*transaction

	// methodMap map[string]reflect.Value

}

// Extract the binding exported functions, which could
// be visited by function name.
// func (ks *TinyKVStore) extractOpFunc() {
// 	ksType := reflect.Type(ks)
// 	for i := 0; i < ksType.NumMethod(); i++ {
// 		mtd := ksType.Method(i)
// 		ks.methodMap[mtd.Name] = mtd.Func
// 	}
// }

// NewTinyKVStore init a tiny KV-Store.
func NewTinyKVStore(addr string) *TinyKVStore {
	log.Printf("Start kvstore service on %s\n", addr)
	ks := &TinyKVStore{flatdata: make(map[string]string),
		hashdata: make(map[string](map[string]string)),
		setdata:  make(map[string](map[string]string)),
		// transMap:  make(map[int64]*Transaction),
		// methodMap: make(map[string]reflect.Value),
		addr: addr,
	}
	// bytesPool: &sync.Pool{
	// 	New: func() {
	// 		buf := make(byte, 1024)
	// 		return bytes.NewBuffer(buf)
	// 	},
	// },
	// ks.extractOpFunc()
	return ks
}

// Serve start the KV-Store service.
func (ks *TinyKVStore) Serve() {
	rpcs := rpc.NewServer()
	rpcs.Register(ks)
	l, e := net.Listen("tcp", ks.addr)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ks.l = l
	go func() {
		for !ks.IsDead() {
			if conn, err := l.Accept(); err == nil {
				if !ks.IsDead() {
					// concurrent processing
					go rpcs.ServeConn(conn)
				} else if err == nil {
					conn.Close()
				}
			} else {
				if !ks.IsDead() {
					log.Fatalln(err.Error())
				}
			}
		}
	}()
}

func (ks *TinyKVStore) IsDead() bool {
	return atomic.LoadInt32(&ks.dead) != 0
}

// Clear the data and close the kvstore service.
func (ks *TinyKVStore) Kill() {
	log.Println("Kill the kvstore")
	atomic.StoreInt32(&ks.dead, 1)
	ks.flatdata = nil
	ks.hashdata = nil
	ks.setdata = nil
	if err := ks.l.Close(); err != nil {
		log.Fatal("Kvsotre rPC server close error:", err)
	}
}

// // Transaction is not thread-safe.
// type transaction struct {
// 	id      int64
// 	actions *list.List
// }

// func (ks *TinyKVStore) TransStart() (tranId int64) {
// 	transId := atomic.AddInt64(&ks.transId, 1)
// 	ks.transMu.Lock()
// 	defer ks.transMu.Unlock()
// 	ks.transMap[transId] = &transaction{id: transId, actions: list.New()}
// 	return
// }

// type TransOpArgs struct {
// 	TransId int64
// 	OpName  string
// 	Args    interface{}
// 	Reply   interface{}
// }

// type TransOpReply struct {
// 	Flag bool
// }

// type TransExecArgs struct {
// 	TransId int64
// }

// type TransExecReply struct {
// 	Flag bool
// }

// func (ks *TinyKVStore) AddTransAction(args *TransOpArgs,
// 	reply *TransOpReply) (err error) {
// 	ks.transMu.RLock()
// 	trans, ok := ks.transMap[args.TransId]
// 	ks.transMu.RUnlock()
// 	if !ok {
// 		reply.Flag = false
// 		return nil
// 	}
// 	met, ok := ks.methodMap[args.OpName]
// 	if !ok {
// 		reply.Flag = false
// 		return nil
// 	}
// 	trans.actions.PushBack(args)
// }

// func (ks *TinyKVStore) ExecTrans(args *TransExecArgs,
// 	reply *TransExecReply) {
// 	ks.transMu.RLock()
// 	trans, ok := ks.transMap[args.TransId]
// 	ks.transMu.RUnlock()
// 	if !ok {
// 		reply.Flag = false
// 		return nil
// 	}
// 	for ele := trans.actions.Front(); ele != nil; ele = ele.Next() {
// 		opArgs := ele.(*TransOpArgs)
// 		// met must be non-nil
// 		met, _ := ks.methodMap[args.OpName]
// 		met.Call([]reflect.Value{reflect.ValueOf(ks),
// 			reflect.ValueOf(opArgs.Args), reflect.ValueOf(opArgs.Reply)})
// 		rVal := reflect.ValueOf(reply)
// 		rBool := rVal.Elem().FieldByName("Flag").Bool()
// 		if !rBool {
// 			// error
// 			reply.Flag = false
// 			break
// 		}

// 	}

// }

// Set the field of the specific hashmap.
// @Flag: true if the hashmap exists, false otherwise.
// @Value: new value.
func (ks *TinyKVStore) HSet(args *HSetArgs, reply *Reply) error {
	// log.Println("hset")
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	hashmap, ok := ks.hashdata[args.Key]
	if !ok {
		hashmap = make(map[string]string)
		ks.hashdata[args.Key] = hashmap
	}
	_, ok = hashmap[args.Field]
	hashmap[args.Field] = args.Value
	reply.Flag, reply.Value = ok, args.Value
	return nil
}

func (ks *TinyKVStore) SDel(args *SDelArgs, reply *NoneStruct) error {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	delete(ks.setdata, args.Key)
	return nil
}

func (ks *TinyKVStore) HDel(args *HDelArgs, reply *NoneStruct) error {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	hashmap, ok := ks.hashdata[args.Key]
	if ok {
		delete(hashmap, args.Field)
	}
	return nil
}

func (ks *TinyKVStore) HDelAll(args *HDelAllArgs, reply *NoneStruct) error {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	delete(ks.hashdata, args.Key)
	return nil
}

func (ks *TinyKVStore) Del(args *DelArgs, reply *NoneStruct) error {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	delete(ks.flatdata, args.Key)
	return nil
}

// Get the field of the specific hashmap.
// @Flag: true if the field exists, false otherwise.
// @Value: self if the field exists, "" otherwise.
func (ks *TinyKVStore) HGet(args *HGetArgs, reply *Reply) error {
	// log.Println("hget")
	ks.RwLock.RLock()
	defer ks.RwLock.RUnlock()
	hashmap, ok := ks.hashdata[args.Key]
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
func (ks *TinyKVStore) HGetAll(args *HGetAllArgs, reply *MapReplyBinary) error {
	// log.Println("hgetall")
	ks.RwLock.RLock()
	defer ks.RwLock.RUnlock()
	hashmap, ok := ks.hashdata[args.Key]
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

// HIncr increase the value of the field of the specific hashmap.
// If the field doesn't exist, then it equals the HSet operation.
// @Flag: true if the field exists before, false otherwise.
// @Value: new value.
func (ks *TinyKVStore) HIncr(args *HIncrArgs, reply *Reply) error {
	// log.Println("hincr")
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	var hashmap map[string]string
	var ok bool
	if hashmap, ok = ks.hashdata[args.Key]; ok {
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
		ks.hashdata[args.Key] = hashmap
	}
	value := strconv.Itoa(args.Diff)
	hashmap[args.Field] = value
	reply.Flag, reply.Value = false, value
	return nil
}

// Add the member into the set of the key.
// @Flag: true if the set exists before, false otherwise.
// @Value: ""
func (ks *TinyKVStore) SAdd(args *SAddArgs, reply *Reply) error {
	// log.Println("sadd")
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	set, ok := ks.setdata[args.Key]
	if !ok {
		set = make(map[string]string)
		ks.setdata[args.Key] = set
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
func (ks *TinyKVStore) SIsMember(args *SIsMemberArgs, reply *Reply) error {
	// log.Println("sismember")
	ks.RwLock.RLock()
	defer ks.RwLock.RUnlock()
	if set, ok := ks.setdata[args.Key]; !ok {
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
func (ks *TinyKVStore) Put(args *PutArgs, reply *Reply) error {
	// log.Println("put")
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	_, ok := ks.flatdata[args.Key]
	ks.flatdata[args.Key] = args.Value
	reply.Flag, reply.Value = ok, args.Value
	return nil
}

// Return value of the specific key.
// @Flag: true if the key exists, false otherwise.
// @Value: self if the key exists, "" otherwise.
func (ks *TinyKVStore) Get(args *GetArgs, reply *Reply) error {
	// log.Println("get")
	ks.RwLock.RLock()
	defer ks.RwLock.RUnlock()
	reply.Value, reply.Flag = ks.flatdata[args.Key]
	return nil
}

// Increase the value of the specific key and the increase can
// be negative.
// @Flag: true if the key exists before, false otherwise.
// @Value: new value.
func (ks *TinyKVStore) Incr(args *IncrArgs, reply *Reply) error {
	// log.Println("incr")
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	if value, ok := ks.flatdata[args.Key]; ok {
		if iVal, err := strconv.Atoi(value); err == nil {
			newVal := strconv.Itoa(iVal + args.Diff)
			ks.flatdata[args.Key] = newVal
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
func (ks *TinyKVStore) CompareAndSet(args *CompareAndSetArgs, reply *Reply) error {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	if ok, _ := ks.compareValue(args.Key, args.Base, args.CompareOp); ok {
		newVal := strconv.Itoa(args.SetValue)
		ks.flatdata[args.Key] = newVal
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
func (ks *TinyKVStore) CompareAndIncr(args *CompareAndIncrArgs, reply *Reply) error {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	if ok, iVal := ks.compareValue(args.Key, args.Base, args.CompareOp); ok {
		newVal := strconv.Itoa(iVal + args.Diff)
		ks.flatdata[args.Key] = newVal
		reply.Flag, reply.Value = true, newVal
	} else {
		reply.Flag, reply.Value = false, ""
	}
	return nil
}

// Compare the value of the specific key and return the result and the value.
// True iff value exists, it is a number and compareOp is true.
func (ks *TinyKVStore) compareValue(key string, base int, compareOp func(int, int) bool) (bool, int) {
	if value, ok := ks.flatdata[key]; ok {
		if iVal, err := strconv.Atoi(value); err == nil && compareOp(iVal, base) {
			return true, iVal
		} else {
			return false, 0
		}
	} else {
		return false, 0
	}
}

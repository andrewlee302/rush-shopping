package kvstore

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
)

type TinyKVStore struct {
	data map[string]string
	l    net.Listener
	addr string

	// RwLock is the publi Read-Write Mutex, which can
	// be used in the extended KV-Store.
	RwLock sync.RWMutex

	dead       int32 // for testing
	unreliable int32 // for testing
}

// NewTinyKVStore init a tiny KV-Store.
func NewTinyKVStore(addr string) *TinyKVStore {
	log.Printf("Start kvstore service on %s\n", addr)
	ks := &TinyKVStore{data: make(map[string]string),
		addr: addr,
	}
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
	ks.data = nil
	if err := ks.l.Close(); err != nil {
		log.Fatal("Kvsotre rPC server close error:", err)
	}
}

func (ks *TinyKVStore) Put(key, value string) (existed bool) {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	_, existed = ks.data[key]
	ks.data[key] = value
	return
}

func (ks *TinyKVStore) Get(key string) (value string, existed bool) {
	ks.RwLock.RLock()
	defer ks.RwLock.RUnlock()
	value, existed = ks.data[key]
	return
}

func (ks *TinyKVStore) Incr(key string, delta int) (newVal string, existed bool, err error) {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	var value string
	if value, existed = ks.data[key]; existed {
		var iVal int
		if iVal, err = strconv.Atoi(value); err == nil {
			newVal = strconv.Itoa(iVal + delta)
			ks.data[key] = newVal
			return
		}
		return
	}
	newVal = strconv.Itoa(delta)
	ks.data[key] = newVal
	return
}

// Put k-v pair.
// @existed: true if the key exists before, false otherwise.
// @Value: new value.
func (ks *TinyKVStore) RPCPut(args *PutArgs, reply *Reply) error {
	reply.Flag, reply.Value = ks.Put(args.Key, args.Value), args.Value
	return nil
}

// Return value of the specific key.
// @Flag: true if the key exists, false otherwise.
// @Value: self if the key exists, "" otherwise.
func (ks *TinyKVStore) RPCGet(args *GetArgs, reply *Reply) error {
	reply.Value, reply.Flag = ks.Get(args.Key)
	return nil
}

// Increase the value of the specific key and the increase can
// be negative. If the key doesn't exist, create it.
// @Flag: true if the key exists before, false otherwise.
// @Value: new value.
func (ks *TinyKVStore) RPCIncr(args *IncrArgs, reply *Reply) (err error) {
	reply.Value, reply.Flag, err = ks.Incr(args.Key, args.Delta)
	return err
}

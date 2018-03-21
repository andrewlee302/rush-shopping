package kv

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
)

type KVStore struct {
	data map[string]string

	// RwLock is the publi Read-Write Mutex, which can
	// be used in the extended KV-Store.
	RwLock sync.RWMutex

	dead       int32 // for testing
	unreliable int32 // for testing
}

// NewKVStore inits a tiny KV-Store.
func NewKVStore() *KVStore {
	ks := &KVStore{data: make(map[string]string)}
	return ks
}

type KVStoreService struct {
	*KVStore
	l       net.Listener
	network string
	addr    string
}

// NewKVStoreService inits a tiny KV-Store service.
func NewKVStoreService(network, addr string) *KVStoreService {
	log.Printf("Start kvstore service on %s\n", addr)
	service := &KVStoreService{KVStore: NewKVStore(),
		network: network, addr: addr,
	}
	return service
}

// Serve start the KV-Store service.
func (service *KVStoreService) Serve() {
	rpcs := rpc.NewServer()
	rpcs.Register(service)
	l, e := net.Listen(service.network, service.addr)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	service.l = l
	go func() {
		for !service.IsDead() {
			if conn, err := l.Accept(); err == nil {
				if !service.IsDead() {
					// concurrent processing
					go rpcs.ServeConn(conn)
				} else if err == nil {
					conn.Close()
				}
			} else {
				if !service.IsDead() {
					log.Fatalln(err.Error())
				}
			}
		}
	}()
}

func (ks *KVStoreService) IsDead() bool {
	return atomic.LoadInt32(&ks.dead) != 0
}

// Clear the data and close the kvstore service.
func (ks *KVStoreService) Kill() {
	log.Println("Kill the kvstore")
	atomic.StoreInt32(&ks.dead, 1)
	ks.data = nil
	if err := ks.l.Close(); err != nil {
		log.Fatal("Kvsotre rPC server close error:", err)
	}
}

func (ks *KVStore) Put(key, value string) (oldValue string, existed bool) {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	oldValue, existed = ks.data[key]
	ks.data[key] = value
	return
}

func (ks *KVStore) Get(key string) (value string, existed bool) {
	ks.RwLock.RLock()
	defer ks.RwLock.RUnlock()
	value, existed = ks.data[key]
	return
}

func (ks *KVStore) Incr(key string, delta int) (newVal string, existed bool, err error) {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	var oldVal string
	if oldVal, existed = ks.data[key]; existed {
		var iOldVal int
		if iOldVal, err = strconv.Atoi(oldVal); err == nil {
			newVal = strconv.Itoa(iOldVal + delta)
			ks.data[key] = newVal
			return
		}
		return
	}
	newVal = strconv.Itoa(delta)
	ks.data[key] = newVal
	return
}

func (ks *KVStore) Del(key string) (existed bool) {
	ks.RwLock.Lock()
	defer ks.RwLock.Unlock()
	if _, existed = ks.data[key]; existed {
		delete(ks.data, key)
	}
	return
}

// Put k-v pair.
// @existed: true if the key exists before, false otherwise.
// @Value: old value.
func (ks *KVStore) RPCPut(args *PutArgs, reply *Reply) error {
	reply.Value, reply.Flag = ks.Put(args.Key, args.Value)
	return nil
}

// Return value of the specific key.
// @Flag: true if the key exists, false otherwise.
// @Value: self if the key exists, "" otherwise.
func (ks *KVStore) RPCGet(args *GetArgs, reply *Reply) error {
	reply.Value, reply.Flag = ks.Get(args.Key)
	return nil
}

// Increase the value of the specific key and the increase can
// be negative. If the key doesn't exist, create it.
// @Flag: true if the key exists before, false otherwise.
// @Value: new value.
// @err: non-nil if the value is numeric.
func (ks *KVStore) RPCIncr(args *IncrArgs, reply *Reply) (err error) {
	reply.Value, reply.Flag, err = ks.Incr(args.Key, args.Delta)
	return err
}

// Del the value of the specific key.
// @Flag: true if the key exists before, false otherwise.
func (ks *KVStore) RPCDel(args *DelArgs, reply *Reply) (err error) {
	reply.Flag = ks.Del(args.Key)
	return nil
}

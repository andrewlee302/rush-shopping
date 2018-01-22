package shopping

import (
	"log"
	"net"
	"net/rpc"
	"rush-shopping/kvstore"
	"strconv"
	"sync"
)

// TransKVStore is the kvstore supported shopping transations.
type TransKVStore struct {
	*kvstore.TinyKVStore

	l        net.Listener
	addr     string
	itemList []Item // real item start from index 1

	transRwLock sync.RWMutex
	// transClient is the client to interact with this kvstore,
	// It is only used in the transaction service.
	// Application will call the transcation handler, which will
	// invoke basic RPC call through transClient. The code handler keeps
	// the code consistency with the application by using RPC call.
	transClient *TransKVClient
}

// AddItemArgs is the argument of the AddItemTrans function.
type AddItemArgs struct {
	CartIDStr string
	UserToken string
	ItemIDStr string
	ItemCnt   int
}

// SubmitOrderArgs is the argument of the SubmitOrderTrans function.
type SubmitOrderArgs struct {
	CartIDStr string
	UserToken string
}

// PayOrderArgs is the argument of the PayOrderTrans function.
type PayOrderArgs struct {
	OrderIDStr string
	UserToken  string
}

// TransReply is the reply of the transcation functions.
type TransReply int

// StartTransKVStore start the transaction-enabled kvstore.
func StartTransKVStore(addr string) *TransKVStore {
	ks := &TransKVStore{TinyKVStore: kvstore.NewTinyKVStore(addr), addr: addr}
	return ks
}

// Serve start the KV-Store service.
func (ks *TransKVStore) Serve() {
	rpcs := rpc.NewServer()
	rpcs.Register(ks)
	l, e := net.Listen("tcp", ks.addr)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ks.l = l
	ks.transClient = NewTransKVClient(ks.addr)

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

func (ks *TransKVStore) LoadItemList(itemsCnt *int, reply *int) error {
	ks.transRwLock.RLock()
	defer ks.transRwLock.RUnlock()
	ks.itemList = make([]Item, 1+*itemsCnt)
	_, priceReply := ks.transClient.HGetAll(ItemsPriceKey)
	_, stockReply := ks.transClient.HGetAll(ItemsStockKey)

	for itemIDStr, stockStr := range stockReply.Value {
		itemID, _ := strconv.Atoi(itemIDStr)
		stock, _ := strconv.Atoi(stockStr)
		price, _ := strconv.Atoi(priceReply.Value[itemIDStr])
		ks.itemList[itemID] = Item{ID: itemID, Price: price, Stock: stock}
	}
	return nil
}

// AddItemTrans is the transcation of adding item process.
func (ks *TransKVStore) AddItemTrans(args *AddItemArgs, transReply *TransReply) error {
	// ks.transRwLock.Lock()
	// defer ks.transRwLock.Unlock()

	// TODO
	cartID, _ := strconv.Atoi(args.CartIDStr)
	cartItemNumKey, cartContentKey := getCartKeys(args.CartIDStr, args.UserToken)
	var reply kvstore.Reply

	// Test whether the cart exisks, and the cart belongs other users.
	var maxCartID = 0
	if _, reply = ks.transClient.Get(CartIDKey); reply.Flag {
		maxCartID, _ = strconv.Atoi(reply.Value)
	}
	if cartID > maxCartID {
		*transReply = TxnNotFound
		return nil
	}

	// Test whether the cart has been ordered.
	orderIDStr := args.UserToken
	if _, reply = ks.transClient.HGet(OrdersKey, orderIDStr); reply.Flag {
		*transReply = TxnNotFound
		return nil
	}

	_, reply = ks.transClient.Get(cartItemNumKey)
	if !reply.Flag {
		*transReply = TxnNotAuth
		return nil
	}

	// Test whether #items in cart exceeds 3.
	if total, _ := strconv.Atoi(reply.Value); total+args.ItemCnt > 3 {
		*transReply = TxnItemOutOfLimit
		return nil
	}

	// Increase the values about the cart.
	_, _ = ks.transClient.Incr(cartItemNumKey, args.ItemCnt)
	ks.transClient.HIncr(cartContentKey, args.ItemIDStr, args.ItemCnt)
	*transReply = TxnOK
	return nil
}

// SubmitOrderTrans is the transcation of submitting order process.
func (ks *TransKVStore) SubmitOrderTrans(args *SubmitOrderArgs, transReply *TransReply) error {
	// ks.transRwLock.Lock()
	// defer ks.transRwLock.Unlock()

	// TODO
	cartID, _ := strconv.Atoi(args.CartIDStr)
	cartItemNumKey, cartContentKey := getCartKeys(args.CartIDStr, args.UserToken)
	var reply kvstore.Reply

	// Test whether the cart exists, it belongs other users,
	// and it is empty.
	var maxCartID = 0
	if _, reply = ks.transClient.Get(CartIDKey); reply.Flag {
		maxCartID, _ = strconv.Atoi(reply.Value)
	}
	if cartID > maxCartID {
		*transReply = TxnNotFound
		return nil
	}
	if _, reply = ks.transClient.Get(cartItemNumKey); !reply.Flag {
		*transReply = TxnNotAuth
		return nil
	}
	num, _ := strconv.Atoi(reply.Value)
	if num == 0 {
		*transReply = TxnCartEmpyt
		return nil
	}

	// Test whether the user has submited an order.
	if _, reply = ks.transClient.HGet(OrdersKey, args.UserToken); reply.Flag {
		*transReply = TxnOrderOutOfLimit
		return nil
	}

	// Test whether the stock of items is enough for the cart.
	total := 0
	var mapReply kvstore.MapReply
	_, mapReply = ks.transClient.HGetAll(cartContentKey)
	for ItemIDStr, itemCntStr := range mapReply.Value {
		_, reply1 := ks.transClient.HGet(ItemsStockKey, ItemIDStr)
		stock, _ := strconv.Atoi(reply1.Value)
		itemID, _ := strconv.Atoi(ItemIDStr)
		itemCnt, _ := strconv.Atoi(itemCntStr)
		total += itemCnt * ks.itemList[itemID].Price
		if stock < itemCnt {
			*transReply = TxnOutOfStock
			return nil
		}
	}

	// Decrease the stock.
	for ItemIDStr, itemCntStr := range mapReply.Value {
		itemCnt, _ := strconv.Atoi(itemCntStr)
		ks.transClient.HIncr(ItemsStockKey, ItemIDStr, 0-itemCnt)
	}

	// Record the order and delete the cart.
	orderIDStr := args.UserToken
	ks.transClient.HSet(OrdersKey, orderIDStr, composeOrderInfo(false, args.CartIDStr, total))

	*transReply = TxnOK
	return nil
}

// PayOrderTrans is the transcation of paying order process.
func (ks *TransKVStore) PayOrderTrans(args *PayOrderArgs, transReply *TransReply) error {
	// ks.transRwLock.Lock()
	// defer ks.transRwLock.Unlock()

	var reply kvstore.Reply

	// TODO
	// Test whether the order exists, or it belongs other users.
	if _, reply = ks.transClient.HGet(OrdersKey, args.OrderIDStr); !reply.Flag {
		*transReply = TxnNotFound
		return nil
	}
	if args.OrderIDStr != args.UserToken {
		*transReply = TxnNotAuth
		return nil
	}

	// Test whether the order have been paid.
	hasPaid, CartIDStr, total := parseOrderInfo(reply.Value)
	if hasPaid {
		*transReply = TxnOrderPaid
		return nil
	}

	// Test whether the balance of the user is sufficient.
	_, reply = ks.transClient.HGet(BalanceKey, args.UserToken)
	balance, _ := strconv.Atoi(reply.Value)
	if balance < total {
		*transReply = TxnBalanceInsufficient
		return nil
	}

	// Decrease the balance of the user.
	ks.transClient.HIncr(BalanceKey, args.UserToken, 0-total)
	ks.transClient.HIncr(BalanceKey, ROOT_USER_TOKEN, total)

	// Record the order.
	ks.transClient.HSet(OrdersKey, args.OrderIDStr, composeOrderInfo(true, CartIDStr, total))

	*transReply = TxnOK
	return nil
}

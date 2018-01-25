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
}

// AddItemArgs is the argument of the AddItemTrans function.
type AddItemArgs struct {
	CartIDStr string
	UserToken string
	ItemID    int
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

func (ks *TransKVStore) LoadItemList(itemsSize *int, reply *int) error {
	ks.transRwLock.RLock()
	defer ks.transRwLock.RUnlock()
	ks.itemList = make([]Item, 1+*itemsSize)
	for itemID := 1; itemID <= *itemsSize; itemID++ {
		value, _ := ks.Get(ItemsPriceKey + ":" + strconv.Itoa(itemID))
		price, _ := strconv.Atoi(value)
		value, _ = ks.Get(ItemsStockKey + ":" + strconv.Itoa(itemID))
		stock, _ := strconv.Atoi(value)
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
	cartItemNumKey, cartDetailKey := getCartKeys(args.CartIDStr, args.UserToken)
	var value string
	var existed bool

	// Test whether the cart exists,
	var maxCartID = 0
	if value, existed = ks.Get(CartIDKey); existed {
		maxCartID, _ = strconv.Atoi(value)
		if cartID > maxCartID || cartID < 1 {
			*transReply = TxnNotFound
			return nil
		}
	} else {
		*transReply = TxnNotFound
		return nil
	}

	// Test whether the cart has been ordered.
	orderIDStr := args.UserToken
	if value, existed = ks.Get(OrderKey + ":" + orderIDStr); existed {
		*transReply = TxnNotFound
		return nil
	}

	// Test whether the cart belongs other users.
	if value, existed = ks.Get(cartItemNumKey); !existed {
		*transReply = TxnNotAuth
		return nil
	}

	// Test whether #items in cart exceeds 3.
	if total, _ := strconv.Atoi(value); total+args.ItemCnt > 3 {
		*transReply = TxnItemOutOfLimit
		return nil
	}

	// Increase the values about the cart.
	ks.Incr(cartItemNumKey, args.ItemCnt)
	value, _ = ks.Get(cartDetailKey)
	cartDetail := parseCartDetail(value)
	cartDetail[args.ItemID] += args.ItemCnt
	ks.Put(cartDetailKey, composeCartDetail(cartDetail))
	*transReply = TxnOK
	return nil
}

// SubmitOrderTrans is the transcation of submitting order process.
func (ks *TransKVStore) SubmitOrderTrans(args *SubmitOrderArgs, transReply *TransReply) error {
	// ks.transRwLock.Lock()
	// defer ks.transRwLock.Unlock()

	// TODO
	cartID, _ := strconv.Atoi(args.CartIDStr)
	cartItemNumKey, cartDetailKey := getCartKeys(args.CartIDStr, args.UserToken)
	var value string
	var existed bool

	// Test whether the cart exists, it belongs other users,
	// and it is empty.
	// Test whether the cart exists,
	var maxCartID = 0
	if value, existed = ks.Get(CartIDKey); existed {
		maxCartID, _ = strconv.Atoi(value)
		if cartID > maxCartID || cartID < 1 {
			*transReply = TxnNotFound
			return nil
		}
	} else {
		*transReply = TxnNotFound
		return nil
	}

	if value, existed = ks.Get(cartItemNumKey); !existed {
		*transReply = TxnNotAuth
		return nil
	}
	num, _ := strconv.Atoi(value)
	if num == 0 {
		*transReply = TxnCartEmpyt
		return nil
	}

	// Test whether the user has submited an order.
	if value, existed = ks.Get(OrderKey + ":" + args.UserToken); existed {
		*transReply = TxnOrderOutOfLimit
		return nil
	}

	// Test whether the stock of items is enough for the cart.
	total := 0
	value, _ = ks.Get(cartDetailKey)
	cartDetail := parseCartDetail(value)
	for itemID, itemCnt := range cartDetail {
		value, _ := ks.Get(ItemsStockKey + ":" + strconv.Itoa(itemID))
		stock, _ := strconv.Atoi(value)
		total += itemCnt * ks.itemList[itemID].Price
		if stock < itemCnt {
			*transReply = TxnOutOfStock
			return nil
		}
	}

	// Decrease the stock.
	for itemID, itemCnt := range cartDetail {
		ks.Incr(ItemsStockKey+":"+strconv.Itoa(itemID), 0-itemCnt)
	}

	// Record the order and delete the cart.
	orderIDStr := args.UserToken
	ks.Put(OrderKey+":"+orderIDStr, composeOrderInfo(false, args.CartIDStr, total))

	*transReply = TxnOK
	return nil
}

// PayOrderTrans is the transcation of paying order process.
func (ks *TransKVStore) PayOrderTrans(args *PayOrderArgs, transReply *TransReply) error {
	// ks.transRwLock.Lock()
	// defer ks.transRwLock.Unlock()

	var value string
	var existed bool

	// TODO
	// Test whether the order exists, or it belongs other users.
	if value, existed = ks.Get(OrderKey + ":" + args.OrderIDStr); !existed {
		*transReply = TxnNotFound
		return nil
	}
	if args.OrderIDStr != args.UserToken {
		*transReply = TxnNotAuth
		return nil
	}

	// Test whether the order have been paid.
	hasPaid, CartIDStr, total := parseOrderInfo(value)
	if hasPaid {
		*transReply = TxnOrderPaid
		return nil
	}

	// Test whether the balance of the user is sufficient.
	value, _ = ks.Get(BalanceKey + ":" + args.UserToken)
	balance, _ := strconv.Atoi(value)
	if balance < total {
		*transReply = TxnBalanceInsufficient
		return nil
	}

	// Decrease the balance of the user.
	ks.Incr(BalanceKey+":"+args.UserToken, 0-total)
	ks.Incr(BalanceKey+":"+ROOT_USER_TOKEN, total)

	// Record the order.
	ks.Put(OrderKey+":"+args.OrderIDStr, composeOrderInfo(true, CartIDStr, total))

	*transReply = TxnOK
	return nil
}

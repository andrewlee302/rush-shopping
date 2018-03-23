package shopping

import (
	"distributed-system/twopc"
	"distributed-system/util"
	"encoding/gob"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

type ShoppingTxnCoordinator struct {
	coord       *twopc.Coordinator
	itemList    []Item // real item start from index 1
	hub         *ShardsClientHub
	keyHashFunc twopc.KeyHashFunc
	timeoutMs   int64

	tasks chan *TxnTask
}

type TxnTask struct {
	txn      *twopc.Txn
	initArgs interface{}
	errCode  int
}

const DefaultTaskMaxSize = 10000

func NewShoppingTxnCoordinator(coord string, ppts []string,
	keyHashFunc twopc.KeyHashFunc, timeoutMs int64) *ShoppingTxnCoordinator {
	sts := &ShoppingTxnCoordinator{coord: twopc.NewCoordinator("tcp", coord, ppts),
		keyHashFunc: keyHashFunc, timeoutMs: timeoutMs,
		hub:   NewShardsClientHub("tcp", ppts, keyHashFunc, 1),
		tasks: make(chan *TxnTask, DefaultTaskMaxSize)}
	go func() {
		for _ = range time.Tick(time.Second * 5) {
			ns := atomic.LoadInt64(&util.RPCCallNs)
			fmt.Println("RPCCall cost ms:", ns/time.Millisecond.Nanoseconds(), ns)
		}
	}()
	sts.coord.RegisterService(sts)
	gob.Register(AddItemTxnInitRet{})
	gob.Register(SubmitOrderTxnInitRet{})
	gob.Register(PayOrderTxnInitRet{})
	go sts.Run()
	return sts
}

// LoadItemList loads item info into the cahce for the slater rapid visiting.
func (stc *ShoppingTxnCoordinator) LoadItemList(itemsSize *int, reply *struct{}) error {
	stc.itemList = make([]Item, 1+*itemsSize)
	for itemID := 1; itemID <= *itemsSize; itemID++ {
		_, reply := stc.hub.Get(ItemsPriceKeyPrefix + strconv.Itoa(itemID))
		price, _ := strconv.Atoi(reply.Value)

		_, reply = stc.hub.Get(ItemsStockKeyPrefix + strconv.Itoa(itemID))
		stock, _ := strconv.Atoi(reply.Value)

		stc.itemList[itemID] = Item{ID: itemID, Price: price, Stock: stock}
	}
	return nil
}

// AddItemArgs is the argument of the AddItemTrans function.
type AddItemArgs struct {
	CartIDStr  string
	UserToken  string
	ItemID     int
	AddItemCnt int
}

type AddItemTxnInitArgs struct {
	OrderKey       string
	CartItemNumKey string
	CartDetailKey  string
	CartIDStr      string
	ItemID         int
	AddItemCnt     int
}

type AddItemTxnInitRet AddItemTxnInitArgs

func AddItemTxnInit(args interface{}) (ret interface{}, errCode int) {
	initArgs := args.(AddItemTxnInitArgs)
	ret = AddItemTxnInitRet(initArgs)
	errCode = 0
	return
}

// AsyncAddItemTxn starts the transcation of adding item to cart.
func (stc *ShoppingTxnCoordinator) AsyncAddItemTxn(args *AddItemArgs, txnID *string) error {
	cartItemNumKey, cartDetailKey := getCartKeys(args.CartIDStr, args.UserToken)
	orderKey := OrderKeyPrefix + args.UserToken

	txn := stc.coord.NewTxn(AddItemTxnInit, stc.keyHashFunc, stc.timeoutMs)
	*txnID = txn.ID

	txn.AddTxnPart(CartIDMaxKey, "CartExist")

	txn.AddTxnPart(orderKey, "CartOrdered")

	txn.AddTxnPart(cartItemNumKey, "CartAuthAndValid")

	txn.AddTxnPart(cartDetailKey, "CartAddItem")

	initArgs := AddItemTxnInitArgs{OrderKey: orderKey,
		CartItemNumKey: cartItemNumKey, CartDetailKey: cartDetailKey,
		CartIDStr: args.CartIDStr, ItemID: args.ItemID,
		AddItemCnt: args.AddItemCnt}
	fmt.Println("AsyncAddItemTxn", initArgs)
	stc.tasks <- &TxnTask{txn: txn, initArgs: initArgs}
	return nil
}

func (stc *ShoppingTxnCoordinator) Run() {
	for task := range stc.tasks {
		task.txn.Start(task.initArgs)
		var reply twopc.TxnState
		stc.coord.SyncTxnEnd(&task.txn.ID, &reply)
		fmt.Println("process", reply.State, reply.ErrCode)
	}
}

// SubmitOrderArgs is the argument of the SubmitOrderTrans function.
type SubmitOrderArgs struct {
	CartIDStr string
	UserToken string
}

type SubmitOrderTxnInitArgs struct {
	stc            *ShoppingTxnCoordinator
	hub            *ShardsClientHub
	OrderKey       string
	CartIDStr      string
	CartItemNumKey string
	CartDetailKey  string
}

type SubmitOrderTxnInitRet struct {
	SubmitOrderTxnInitArgs
	CartDetailStr string
	Total         int
}

func SubmitOrderTxnInit(args interface{}) (ret interface{}, errCode int) {
	initArgs := args.(*SubmitOrderTxnInitArgs)

	ok, reply := initArgs.hub.Get(initArgs.CartDetailKey)
	if !ok {
		errCode = -3
	}
	cartDetailStr := reply.Value
	cartDetail := parseCartDetail(cartDetailStr)
	total := 0
	for itemID, itemCnt := range cartDetail {
		total += itemCnt * initArgs.stc.itemList[itemID].Price
	}
	ret = &SubmitOrderTxnInitRet{SubmitOrderTxnInitArgs: *initArgs,
		CartDetailStr: cartDetailStr, Total: total}
	errCode = 0
	return
}

// AsyncSubmitOrderTxn submit the transcation of submiting the order.
func (stc *ShoppingTxnCoordinator) AsyncSubmitOrderTxn(args *SubmitOrderArgs, txnID *string) error {
	cartItemNumKey, cartDetailKey := getCartKeys(args.CartIDStr, args.UserToken)
	orderKey := OrderKeyPrefix + args.UserToken

	txn := stc.coord.NewTxn(SubmitOrderTxnInit, stc.keyHashFunc, stc.timeoutMs)
	*txnID = txn.ID

	txn.AddTxnPart(CartIDMaxKey, "CartExist2")

	txn.AddTxnPart(cartItemNumKey, "CartAuthAndEmpty")

	txn.BroadcastTxnPart("ItemsStockMinus")

	txn.AddTxnPart(orderKey, "OrderRecord")

	// TODO?
	// client: client,
	initArgs := &SubmitOrderTxnInitArgs{stc: stc, hub: stc.hub,
		CartIDStr: args.CartIDStr, CartItemNumKey: cartItemNumKey,
		CartDetailKey: cartDetailKey,
		OrderKey:      orderKey}

	fmt.Println("AsyncSubmitOrderTxn", initArgs)
	stc.tasks <- &TxnTask{txn: txn, initArgs: initArgs}
	return nil
}

// PayOrderArgs is the argument of the PayOrderTrans function.
type PayOrderArgs struct {
	OrderIDStr string
	UserToken  string
}

type PayOrderTxnInitArgs struct {
	hub            *ShardsClientHub
	OrderIDStr     string
	UserToken      string
	OrderKey       string
	BalanceKey     string
	RootBalanceKey string
}

type PayOrderTxnInitRet struct {
	PayOrderTxnInitArgs
	OrderInfo string
	Delta     int
}

func PayOrderTxnInit(args interface{}) (ret interface{}, errCode int) {
	initArgs := args.(*PayOrderTxnInitArgs)

	if initArgs.OrderIDStr != initArgs.UserToken {
		errCode = TxnNotAuth
		return
	}

	// Test whether the order exists, or it belongs other users.
	ok, reply := initArgs.hub.Get(initArgs.OrderKey)
	if !ok {
		errCode = -3
		return
	}
	if !reply.Flag {
		errCode = TxnNotFound
		return
	}

	// Test whether the order have been paid.
	hasPaid, CartIDStr, total := parseOrderInfo(reply.Value)
	if hasPaid {
		errCode = TxnOrderPaid
		return
	}
	orderInfo := composeOrderInfo(true, CartIDStr, total)
	ret = PayOrderTxnInitRet{PayOrderTxnInitArgs: *initArgs,
		OrderInfo: orderInfo, Delta: total}
	errCode = 0
	return
}

// AsyncPayOrderTxn submit the transcation of paying the order.
func (stc *ShoppingTxnCoordinator) AsyncPayOrderTxn(args *PayOrderArgs, txnID *string) error {
	balanceKey := BalanceKeyPrefix + args.UserToken
	rootBalanceKey := BalanceKeyPrefix + RootUserToken
	orderKey := OrderKeyPrefix + args.OrderIDStr

	txn := stc.coord.NewTxn(PayOrderTxnInit, stc.keyHashFunc, stc.timeoutMs)
	*txnID = txn.ID

	txn.AddTxnPart(balanceKey, "PayMinus")

	txn.AddTxnPart(rootBalanceKey, "PayAdd")

	txn.AddTxnPart(orderKey, "PayRecord")

	initArgs := &PayOrderTxnInitArgs{hub: stc.hub,
		BalanceKey: balanceKey, RootBalanceKey: rootBalanceKey,
		UserToken: args.UserToken, OrderIDStr: args.OrderIDStr}

	fmt.Println("AsyncPayOrderTxn", initArgs)
	stc.tasks <- &TxnTask{txn: txn, initArgs: initArgs}
	return nil
}

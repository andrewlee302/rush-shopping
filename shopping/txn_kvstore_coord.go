package shopping

import (
	"distributed-system/twopc"
	"encoding/gob"
	"fmt"
	"strconv"
)

type ShoppingTxnCoordinator struct {
	coord        *twopc.Coordinator
	itemList     []Item // real item start from index 1
	shardsClient *ShardsClient
	keyHashFunc  twopc.KeyHashFunc
	timeoutMs    int64
}

func NewShoppingTxnCoordinator(coord string, ppts []string,
	keyHashFunc twopc.KeyHashFunc, timeoutMs int64) *ShoppingTxnCoordinator {
	sts := &ShoppingTxnCoordinator{coord: twopc.NewCoordinator("tcp", coord, ppts),
		keyHashFunc: keyHashFunc, timeoutMs: timeoutMs,
		shardsClient: NewShardsClient(ppts, keyHashFunc)}
	sts.coord.RegisterService(sts)
	gob.Register(AddItemTxnInitRet{})
	gob.Register(SubmitOrderTxnInitRet{})
	gob.Register(PayOrderTxnInitRet{})
	return sts
}

// LoadItemList loads item info into the cahce for the slater rapid visiting.
func (stc *ShoppingTxnCoordinator) LoadItemList(itemsSize *int, reply *struct{}) error {
	stc.itemList = make([]Item, 1+*itemsSize)
	for itemID := 1; itemID <= *itemsSize; itemID++ {
		_, reply := stc.shardsClient.Get(ItemsPriceKeyPrefix + strconv.Itoa(itemID))
		price, _ := strconv.Atoi(reply.Value)

		_, reply = stc.shardsClient.Get(ItemsStockKeyPrefix + strconv.Itoa(itemID))
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

// StartAddItemTxn starts the transcation of adding item to cart.
func (stc *ShoppingTxnCoordinator) StartAddItemTxn(args *AddItemArgs, txnID *string) error {
	cartItemNumKey, cartDetailKey := getCartKeys(args.CartIDStr, args.UserToken)
	orderKey := OrderKeyPrefix + args.UserToken

	txn := stc.coord.NewTxn(AddItemTxnInit, stc.keyHashFunc, stc.timeoutMs)
	*txnID = txn.ID

	txn.AddTxnPart(CartIDMaxKey, "CartExist")

	txn.AddTxnPart(orderKey, "CartOrdered")

	txn.AddTxnPart(cartItemNumKey, "CartAuthAndValid")

	txn.AddTxnPart(cartDetailKey, "CartAddItem")

	addItemTxnInitArgs := AddItemTxnInitArgs{OrderKey: orderKey,
		CartItemNumKey: cartItemNumKey, CartDetailKey: cartDetailKey,
		CartIDStr: args.CartIDStr, ItemID: args.ItemID,
		AddItemCnt: args.AddItemCnt}
	fmt.Println("StartAddItemTxn", addItemTxnInitArgs)
	txn.Start(addItemTxnInitArgs)
	return nil
}

// SubmitOrderArgs is the argument of the SubmitOrderTrans function.
type SubmitOrderArgs struct {
	CartIDStr string
	UserToken string
}

type SubmitOrderTxnInitArgs struct {
	stc            *ShoppingTxnCoordinator
	client         *ShardsClient
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

	ok, reply := initArgs.client.Get(initArgs.CartDetailKey)
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

// StartOrderTxn starts the transcation of submiting the order.
func (stc *ShoppingTxnCoordinator) StartSubmitOrderTxn(args *SubmitOrderArgs, txnID *string) error {
	cartItemNumKey, cartDetailKey := getCartKeys(args.CartIDStr, args.UserToken)
	orderKey := OrderKeyPrefix + args.UserToken

	txn := stc.coord.NewTxn(SubmitOrderTxnInit, stc.keyHashFunc, stc.timeoutMs)
	*txnID = txn.ID

	txn.AddTxnPart(CartIDMaxKey, "CartExist2")

	txn.AddTxnPart(cartItemNumKey, "CartAuthAndEmpty")

	// txn.AddTxnPart(orderKey, "OrderIsSubmited")

	txn.BroadcastTxnPart("ItemsStockMinus")

	txn.AddTxnPart(orderKey, "OrderRecord")

	// TODO?
	// client: client,
	initArgs := &SubmitOrderTxnInitArgs{stc: stc, client: stc.shardsClient,
		CartIDStr: args.CartIDStr, CartItemNumKey: cartItemNumKey,
		CartDetailKey: cartDetailKey,
		OrderKey:      orderKey}
	txn.Start(initArgs)
	return nil
}

// PayOrderArgs is the argument of the PayOrderTrans function.
type PayOrderArgs struct {
	OrderIDStr string
	UserToken  string
}

type PayOrderTxnInitArgs struct {
	client         *ShardsClient
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
	ok, reply := initArgs.client.Get(initArgs.OrderKey)
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

// StartPayOrderTxn starts the transcation of paying the order.
func (stc *ShoppingTxnCoordinator) StartPayOrderTxn(args *PayOrderArgs, txnID *string) error {
	balanceKey := BalanceKeyPrefix + args.UserToken
	rootBalanceKey := BalanceKeyPrefix + RootUserToken
	orderKey := OrderKeyPrefix + args.OrderIDStr

	txn := stc.coord.NewTxn(PayOrderTxnInit, stc.keyHashFunc, stc.timeoutMs)
	*txnID = txn.ID

	txn.AddTxnPart(balanceKey, "PayMinus")

	txn.AddTxnPart(rootBalanceKey, "PayAdd")

	txn.AddTxnPart(orderKey, "PayRecord")

	initArgs := &PayOrderTxnInitArgs{client: stc.shardsClient,
		BalanceKey: balanceKey, RootBalanceKey: rootBalanceKey,
		UserToken: args.UserToken, OrderIDStr: args.OrderIDStr}
	txn.Start(initArgs)
	return nil
}

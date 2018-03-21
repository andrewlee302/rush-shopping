package shopping

import (
	"distributed-system/twopc"
	"fmt"
	"strconv"
)

func (skv *ShoppingTxnKVStore) CartExist(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	// fmt.Println("CartExist start", initRet)
	// defer fmt.Println("CartExist end", errCode)
	args := initRet.(AddItemTxnInitRet)
	rbf = twopc.BlankRollbackFunc

	var value string
	var existed bool

	cartID, _ := strconv.Atoi(args.CartIDStr)
	// Test whether the cart exists,
	var maxCartID = 0
	if value, existed = skv.Get(CartIDMaxKey); existed {
		maxCartID, _ = strconv.Atoi(value)
		if cartID > maxCartID || cartID < 1 {
			errCode = TxnNotFound
			return
		}
	} else {
		errCode = TxnNotFound
		return
	}
	errCode = TxnOK
	return
}

func (skv *ShoppingTxnKVStore) CartOrdered(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	// fmt.Println("CartOrdered start:", initRet)
	// defer fmt.Println("CartOrdered end", errCode)

	args := initRet.(AddItemTxnInitRet)
	rbf = twopc.BlankRollbackFunc

	var existed bool
	// Test whether the cart has been ordered.
	if _, existed = skv.Get(args.OrderKey); existed {
		errCode = TxnNotFound
		return
	}
	errCode = TxnOK
	return
}

type CartAuthAndValidArgs struct {
	CartItemNumKey string
	AddItemCnt     int
}

func (skv *ShoppingTxnKVStore) CartAuthAndValid(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	// fmt.Println("CartAuthAndValid start:", initRet)
	// defer func() { fmt.Println("CartAuthAndValid end:", errCode) }()

	args := initRet.(AddItemTxnInitRet)
	rbf = twopc.BlankRollbackFunc

	var value string
	var existed bool

	// Test whether the cart belongs other users.
	if value, existed = skv.Get(args.CartItemNumKey); !existed {
		errCode = TxnNotAuth
		return
	}

	// Test whether #items in cart exceeds 3.
	total, _ := strconv.Atoi(value)
	// fmt.Println("total:", total)
	if total+args.AddItemCnt > 3 {
		errCode = TxnItemOutOfLimit
		return
	}

	// Increase the values about the cart.
	skv.Incr(args.CartItemNumKey, args.AddItemCnt)
	rbf = twopc.RollbackFunc(func() {
		skv.Incr(args.CartItemNumKey, 0-args.AddItemCnt)
	})
	errCode = TxnOK
	return
}

// type TxnIncrArgs struct {
// 	Key   string
// 	Delta int
// }

// func (skv *ShoppingTxnKVStore) TxnIncr(args interface{}, initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
// 	incrArgs := args.(IncrArgs)
// 	rbf = twopc.RollbackFunc(func() {
// 		skv.Incr(incrArgs.Key, 0-incrArgs.Delta)
// 	})
// 	skv.Incr(incrArgs.Key, incrArgs.Delta)
// 	errCode = 0
// 	return
// }

func (skv *ShoppingTxnKVStore) CartAddItem(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	// fmt.Println("CartAddItem start:", initRet)
	// defer func() { fmt.Println("CartAddItem end", errCode) }()

	args := initRet.(AddItemTxnInitRet)

	value, _ := skv.Get(args.CartDetailKey)
	rbf = twopc.RollbackFunc(func() {
		skv.Put(args.CartDetailKey, value)
	})
	cartDetail := parseCartDetail(value)
	cartDetail[args.ItemID] += args.AddItemCnt
	skv.Put(args.CartDetailKey, composeCartDetail(cartDetail))
	errCode = TxnOK
	return
}

// ===============================================================================
func (skv *ShoppingTxnKVStore) CartExist2(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	// fmt.Println("CartExist2 start:", initRet)
	// defer func() { fmt.Println("CartExist2 end:", errCode) }()
	args := initRet.(SubmitOrderTxnInitRet)
	rbf = twopc.BlankRollbackFunc

	var value string
	var existed bool

	cartID, _ := strconv.Atoi(args.CartIDStr)
	// Test whether the cart exists,
	var maxCartID = 0
	if value, existed = skv.Get(CartIDMaxKey); existed {
		maxCartID, _ = strconv.Atoi(value)
		if cartID > maxCartID || cartID < 1 {
			errCode = TxnNotFound
			return
		}
	} else {
		errCode = TxnNotFound
		return
	}
	errCode = TxnOK
	return
}

func (skv *ShoppingTxnKVStore) CartAuthAndEmpty(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	// fmt.Println("CartAuthAndEmpty start:", initRet)
	// defer func() { fmt.Println("CartAuthAndEmpty end:", errCode) }()
	args := initRet.(SubmitOrderTxnInitRet)
	rbf = twopc.BlankRollbackFunc

	var value string
	var existed bool
	// Test whether the cart belongs other users.
	if value, existed = skv.Get(args.CartItemNumKey); !existed {
		errCode = TxnNotAuth
		return
	}

	// Test whether the cart is empty.
	num, _ := strconv.Atoi(value)
	if num == 0 {
		errCode = TxnCartEmpyt
		return
	}
	errCode = TxnOK
	return
}

// func (skv *ShoppingTxnKVStore) OrderIsSubmited(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
// 	fmt.Println("OrderIsSubmited start:", initRet)
// 	defer func() { fmt.Println("OrderIsSubmited end:", errCode) }()

// 	args := initRet.(SubmitOrderTxnInitRet)
// 	rbf = twopc.BlankRollbackFunc

// var existed bool

// 	// Test whether the user has submited an order.
// 	if _, existed = skv.Get(args.OrderKey); existed {
// 		errCode = TxnOrderOutOfLimit
// 		return
// 	}
// 	errCode = TxnOK
// 	return
// }

// Broadcast mode
// Must check whether the key does exist or not.
func (skv *ShoppingTxnKVStore) ItemsStockMinus(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	// fmt.Println("ItemsStockMinus start:", initRet)
	// defer func() { fmt.Println("ItemsStockMinus end:", errCode) }()
	args := initRet.(SubmitOrderTxnInitRet)
	cartDetail := parseCartDetail(args.CartDetailStr)
	errCode = TxnOK
	for itemID, itemCnt := range cartDetail {
		itemsStockKey := ItemsStockKeyPrefix + strconv.Itoa(itemID)
		if _, existed := skv.Get(itemsStockKey); existed {
			newValue, _, _ := skv.Incr(itemsStockKey, 0-itemCnt)
			iNewValue, _ := strconv.Atoi(newValue)
			if iNewValue < 0 {
				errCode = TxnOutOfStock

			}
		}
	}

	rbf = twopc.RollbackFunc(func() {
		for itemID, itemCnt := range cartDetail {
			itemsStockKey := ItemsStockKeyPrefix + strconv.Itoa(itemID)
			if _, existed := skv.Get(itemsStockKey); existed {
				skv.Incr(itemsStockKey, itemCnt)
			}
		}

	})

	return
}

func (skv *ShoppingTxnKVStore) OrderRecord(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	fmt.Println("OrderRecord start:", initRet)
	defer func() { fmt.Println("OrderRecord end:", errCode) }()
	args := initRet.(SubmitOrderTxnInitRet)

	// Record the order and delete the cart.
	oldValue, existed := skv.Put(args.OrderKey, composeOrderInfo(false, args.CartIDStr, args.Total))
	if existed {
		errCode = TxnOrderOutOfLimit
		rbf = twopc.RollbackFunc(func() {
			skv.Put(args.OrderKey, oldValue)
		})
		return
	} else {
		rbf = twopc.RollbackFunc(func() {
			skv.Del(args.OrderKey)
		})
	}
	errCode = TxnOK
	return
}

// ===============================================================================

func (skv *ShoppingTxnKVStore) PayMinus(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	fmt.Println("PayMinus start:", initRet)
	defer func() { fmt.Println("PayMinus end:", errCode) }()
	args := initRet.(PayOrderTxnInitRet)
	rbf = twopc.BlankRollbackFunc

	rbf = twopc.RollbackFunc(func() {
		skv.Incr(args.BalanceKey, args.Delta)
	})
	// Decrease the balance of the user.
	newVal, _, _ := skv.Incr(args.BalanceKey, 0-args.Delta)
	iNewVal, _ := strconv.Atoi(newVal)
	if iNewVal < 0 {
		errCode = TxnBalanceInsufficient
		return
	}
	errCode = TxnOK
	return
}

func (skv *ShoppingTxnKVStore) PayAdd(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	fmt.Println("PayAdd start:", initRet)
	defer func() { fmt.Println("PayAdd end:", errCode) }()
	args := initRet.(PayOrderTxnInitRet)
	rbf = twopc.RollbackFunc(func() {
		skv.Incr(args.RootBalanceKey, 0-args.Delta)
	})
	skv.Incr(args.RootBalanceKey, args.Delta)
	errCode = TxnOK
	return
}

type PayRecordArgs struct {
	OrderIDStr, OrderInfo string
}

func (skv *ShoppingTxnKVStore) PayRecord(initRet interface{}) (errCode int, rbf twopc.Rollbacker) {
	fmt.Println("PayRecord start:", initRet)
	defer func() { fmt.Println("PayRecord end:", errCode) }()
	args := initRet.(PayOrderTxnInitRet)

	// Record the order.
	oldValue, existed := skv.Put(args.OrderKey, args.OrderInfo)

	rbf = twopc.RollbackFunc(func() {
		if existed {
			skv.Put(args.OrderKey, oldValue)
		} else {
			skv.Del(args.OrderKey)
		}
	})

	errCode = TxnOK
	return
}

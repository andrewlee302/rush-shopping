package shopping

import (
	"rush-shopping/kvstore"
	"strconv"
)

// resident memory
var (
	ItemList  []Item                   // real item start from index 1
	UserMap   map[string]UserIdAndPass // map[name]password
	MaxItemID int
	MaxUserID int
)

// transaction problem
func AddFoodTrans(cartIdStr, userToken, itemIdStr string, itemCnt int) int {
	cartId, _ := strconv.Atoi(cartIdStr)
	cartTotalKey, cartContentKey := getCartKeys(cartIdStr, userToken)
	var reply kvstore.Reply

	// Test whether the cart exists, and the cart belongs other users.
	var maxCartId = 0
	if _, reply = kvClient.Get(CART_ID_KEY); reply.Flag {
		maxCartId, _ = strconv.Atoi(reply.Value)
	}
	if cartId > maxCartId {
		return 1
	}
	if _, reply = kvClient.Get(cartTotalKey); !reply.Flag {
		return 2
	}

	// Test whether #items in cart exceeds 3.
	_, reply = kvClient.Get(cartTotalKey)
	if total, _ := strconv.Atoi(reply.Value); total+itemCnt > 3 {
		return 3
	}

	// Increase the values about the cart.
	_, _ = kvClient.Incr(cartTotalKey, itemCnt)
	kvClient.HIncr(cartContentKey, itemIdStr, itemCnt)
	return 0
}

// transaction problem
func SubmitOrderTrans(cartIdStr, userToken string) int {
	cartId, _ := strconv.Atoi(cartIdStr)
	cartTotalKey, cartContentKey := getCartKeys(cartIdStr, userToken)
	var reply kvstore.Reply

	// Test whether the cart exists, it belongs other users,
	// and it is empty.
	var maxCartId = 0
	if _, reply = kvClient.Get(CART_ID_KEY); reply.Flag {
		maxCartId, _ = strconv.Atoi(reply.Value)
	}
	if cartId > maxCartId {
		return 1
	}
	if _, reply = kvClient.Get(cartTotalKey); !reply.Flag {
		return 2
	}
	total, _ := strconv.Atoi(reply.Value)
	if total == 0 {
		return 3
	}

	// Test whether the only order of one user has been submited.
	if _, reply = kvClient.HGet(ORDERS_KEY, userToken); reply.Flag {
		return 5
	}

	// Test whether the stock of items is enough for the cart.
	var mapReply kvstore.MapReply
	_, mapReply = kvClient.HGetAll(cartContentKey)
	for itemIdStr, itemCntStr := range mapReply.Value {
		_, reply1 := kvClient.HGet(ITEMS_KEY, itemIdStr)
		stock, _ := strconv.Atoi(reply1.Value)
		itemCnt, _ := strconv.Atoi(itemCntStr)
		if stock < itemCnt {
			return 4
		}
	}

	// Decrease the stock.
	for itemIdStr, itemCntStr := range mapReply.Value {
		itemCnt, _ := strconv.Atoi(itemCntStr)
		kvClient.HIncr(ITEMS_KEY, itemIdStr, 0-itemCnt)
	}

	// Record the order.
	kvClient.HSet(ORDERS_KEY, userToken, cartIdStr)

	return 0
}

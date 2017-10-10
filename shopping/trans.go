package shopping

import (
	"rush-shopping/kvstore"
	"strconv"
)

// transaction problem
func (ss *ShopServer) addFoodTrans(cartIdStr, userToken, itemIdStr string, itemCnt int, kvClient *kvstore.Client) int {
	cartId, _ := strconv.Atoi(cartIdStr)
	cartItemNumKey, cartContentKey := getCartKeys(cartIdStr, userToken)
	var reply kvstore.Reply

	// Test whether the cart exists, and the cart belongs other users.
	var maxCartId = 0
	if _, reply = kvClient.Get(CART_ID_KEY); reply.Flag {
		maxCartId, _ = strconv.Atoi(reply.Value)
	}
	if cartId > maxCartId {
		return RET_NOT_FOUND
	}
	if _, reply = kvClient.Get(cartItemNumKey); !reply.Flag {
		return RET_NOT_AUTH
	}

	// Test whether #items in cart exceeds 3.
	_, reply = kvClient.Get(cartItemNumKey)
	if total, _ := strconv.Atoi(reply.Value); total+itemCnt > 3 {
		return RET_ITEM_OUT_OF_LIMIT
	}

	// Increase the values about the cart.
	_, _ = kvClient.Incr(cartItemNumKey, itemCnt)
	kvClient.HIncr(cartContentKey, itemIdStr, itemCnt)
	return RET_OK
}

// transaction problem
func (ss *ShopServer) submitOrderTrans(cartIdStr, userToken string, kvClient *kvstore.Client) int {
	cartId, _ := strconv.Atoi(cartIdStr)
	cartItemNumKey, cartContentKey := getCartKeys(cartIdStr, userToken)
	var reply kvstore.Reply

	// Test whether the cart exists, it belongs other users,
	// and it is empty.
	var maxCartId = 0
	if _, reply = kvClient.Get(CART_ID_KEY); reply.Flag {
		maxCartId, _ = strconv.Atoi(reply.Value)
	}
	if cartId > maxCartId {
		return RET_NOT_FOUND
	}
	if _, reply = kvClient.Get(cartItemNumKey); !reply.Flag {
		return RET_NOT_AUTH
	}
	num, _ := strconv.Atoi(reply.Value)
	if num == 0 {
		return RET_CART_EMPTY
	}

	// Test whether the only order of one user has been submited.
	if _, reply = kvClient.HGet(ORDERS_KEY, userToken); reply.Flag {
		return RET_ORDER_OUT_OF_LIMIT
	}

	// Test whether the stock of items is enough for the cart.
	total := 0
	var mapReply kvstore.MapReply
	_, mapReply = kvClient.HGetAll(cartContentKey)
	for itemIdStr, itemCntStr := range mapReply.Value {
		_, reply1 := kvClient.HGet(ITEMS_KEY, itemIdStr)
		stock, _ := strconv.Atoi(reply1.Value)
		itemId, _ := strconv.Atoi(itemIdStr)
		itemCnt, _ := strconv.Atoi(itemCntStr)
		total += itemCnt * ss.ItemList[itemId].Price
		if stock < itemCnt {
			return RET_OUT_OF_STOCK
		}
	}

	// Decrease the stock.
	for itemIdStr, itemCntStr := range mapReply.Value {
		itemCnt, _ := strconv.Atoi(itemCntStr)
		kvClient.HIncr(ITEMS_KEY, itemIdStr, 0-itemCnt)
	}

	// Record the order.
	orderIdStr := userToken
	kvClient.HSet(ORDERS_KEY, orderIdStr, composeOrderInfo(false, cartIdStr, total))

	return RET_OK
}

// transaction problem
func (ss *ShopServer) payOrderTrans(orderIdStr, userToken string, kvClient *kvstore.Client) int {
	var reply kvstore.Reply

	// Test whether the order exists, or it belongs other users.
	if _, reply = kvClient.HGet(ORDERS_KEY, orderIdStr); !reply.Flag {
		return RET_NOT_FOUND
	}
	if orderIdStr != userToken {
		return RET_NOT_AUTH
	}

	// Test whether the order have been paid.
	hasPaid, cartIdStr, total := parseOrderInfo(reply.Value)
	if hasPaid {
		return RET_ORDER_PAID
	}

	// Test whether the balance of the user is sufficient.
	_, reply = kvClient.HGet(BALANCE_KEY, userToken)
	balance, _ := strconv.Atoi(reply.Value)
	if balance < total {
		return RET_BALANCE_INSUFFICIENT
	}

	// Decrease the balance of the user.
	kvClient.HIncr(BALANCE_KEY, userToken, 0-total)
	kvClient.HIncr(BALANCE_KEY, ROOT_USER_TOKEN, total)

	// Record the order.
	kvClient.HSet(ORDERS_KEY, orderIdStr, composeOrderInfo(true, cartIdStr, total))

	return RET_OK
}

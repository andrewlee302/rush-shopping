package shopping

// The type of ID for Item and User is int.
// To differ the two from others, we specify the
// field IDStr to indicate the its type is string,
// otherwise int.

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type Item struct {
	ID    int `json:"id"`
	Price int `json:"price"`
	Stock int `json:"stock"`
}

type LoginJson struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type Order struct {
	// if total < 0, then is a order
	IDStr   string      `json:"id"`
	Items   []ItemCount `json:"items"`
	Total   int         `json:"total"`
	HasPaid bool        `json:"paid"`
}

type ItemCount struct {
	ItemID int `json:"item_id"`
	Count  int `json:"count"`
}

// for GET /admin/orders
type OrderDetail struct {
	Order
	UserID int `json:"user_id"`
}

type CartIDJson struct {
	IDStr string `json:"cart_id"`
}

type OrderIDJson struct {
	IDStr string `json:"order_id"`
}

type UserIDAndPass struct {
	ID       int
	Password string
}

// Retrun the item-num-key and content-key in kvstore.
func getCartKeys(cartIDStr, token string) (cartItemNumKey, cartDetailKey string) {
	cartItemNumKey = "cart_n:" + cartIDStr + ":" + token
	cartDetailKey = "cart_d:" + cartIDStr + ":" + token
	return
}

func userID2Token(userID int) string {
	return strconv.Itoa(userID)
}

func token2UserID(token string) int {
	if id, err := strconv.Atoi(token); err == nil {
		return id
	} else {
		panic(err)
	}
}

func composeOrderInfo(hasPaid bool, cartIDStr string, total int) string {
	var info [3]string
	if hasPaid {
		info[0] = ORDER_PAID_FLAG
	} else {
		info[0] = ORDER_UNPAID_FLAG
	}
	info[1] = cartIDStr
	info[2] = strconv.Itoa(total)
	return strings.Join(info[:], ",")
}

func parseOrderInfo(orderInfo string) (hasPaid bool, cartIDStr string, total int) {
	info := strings.Split(orderInfo, ",")
	if info[0] == ORDER_PAID_FLAG {
		hasPaid = true
	} else {
		hasPaid = false
	}
	cartIDStr = info[1]
	total, _ = strconv.Atoi(info[2])
	return
}

func parseCartDetail(cartDetailStr string) (detail map[int]int) {
	detail = make(map[int]int)
	if cartDetailStr == "" {
		return
	}
	itemStrs := strings.Split(cartDetailStr, ";")
	for _, itemStr := range itemStrs {
		info := strings.Split(itemStr, ":")
		if len(info) != 2 {
			fmt.Println("here", len(cartDetailStr), itemStr, len(itemStrs))
		}
		itemID, _ := strconv.Atoi(info[0])
		itemCnt, _ := strconv.Atoi(info[1])
		detail[itemID] = itemCnt
	}
	return
}

func composeCartDetail(cartDetail map[int]int) (cartDetailStr string) {
	var buffer bytes.Buffer
	for itemID, itemCnt := range cartDetail {
		buffer.WriteString(strconv.Itoa(itemID))
		buffer.WriteString(":")
		buffer.WriteString(strconv.Itoa(itemCnt))
		buffer.WriteString(";")
	}
	buffer.Truncate(buffer.Len() - 1)
	return buffer.String()
}

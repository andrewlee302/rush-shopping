package shopping

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"rush-shopping/kvstore"
	"strconv"
	"strings"
	"time"
)

const (
	LOGIN                 = "/login"
	QUERY_ITEM            = "/items"
	CREATE_CART           = "/carts"
	Add_ITEM              = "/carts/"
	SUBMIT_OR_QUERY_ORDER = "/orders"
	QUERY_ALL_ORDERS      = "/admin/orders"
)

const (
	ORDERS_KEY  = "orders"
	ITEMS_KEY   = "items"
	CART_ID_KEY = "cartId"
	BALANCE_KEY = "balance"
)

const (
	ORDER_PAID_FLAG   = "P" // have been paid
	ORDER_UNPAID_FLAG = "W" // wait to be paid
)

const ROOT_USER_ID = 0

var ROOT_USER_TOKEN = userId2Token(ROOT_USER_ID)

const (
	RET_OK = iota
	RET_NOT_FOUND
	RET_NOT_AUTH
	RET_CART_EMPTY
	RET_OUT_OF_STOCK
	RET_ITEM_OUT_OF_LIMIT
	RET_ORDER_OUT_OF_LIMIT
	RET_ORDER_PAID
	RET_BALANCE_INSUFFICIENT
)

var (
	USER_AUTH_FAIL_MSG       = []byte("{\"code\":\"USER_AUTH_FAIL\",\"message\":\"用户名或密码错误\"}")
	MALFORMED_JSON_MSG       = []byte("{\"code\": \"MALFORMED_JSON\",\"message\": \"格式错误\"}")
	EMPTY_REQUEST_MSG        = []byte("{\"code\": \"EMPTY_REQUEST\",\"message\": \"请求体为空\"}")
	INVALID_ACCESS_TOKEN_MSG = []byte("{\"code\": \"INVALID_ACCESS_TOKEN\",\"message\": \"无效的令牌\"}")
	CART_NOT_FOUND_MSG       = []byte("{\"code\": \"CART_NOT_FOUND\", \"message\": \"篮子不存在\"}")
	CART_EMPTY               = []byte("{\"code\": \"CART_EMPTY\", \"message\": \"购物车为空\"}")
	NOT_AUTHORIZED_CART_MSG  = []byte("{\"code\": \"NOT_AUTHORIZED_TO_ACCESS_CART\",\"message\": \"无权限访问指定的篮子\"}")
	ITEM_OUT_OF_LIMIT_MSG    = []byte("{\"code\": \"ITEM_OUT_OF_LIMIT\",\"message\": \"篮子中物品数量超过了三个\"}")
	ITEM_NOT_FOUND_MSG       = []byte("{\"code\": \"ITEM_NOT_FOUND\",\"message\": \"物品不存在\"}")
	ITEM_OUT_OF_STOCK_MSG    = []byte("{\"code\": \"ITEM_OUT_OF_STOCK\", \"message\": \"物品库存不足\"}")
	ORDER_OUT_OF_LIMIT_MSG   = []byte("{\"code\": \"ORDER_OUT_OF_LIMIT\",\"message\": \"每个用户只能下一单\"}")

	ORDER_NOT_FOUND_MSG      = []byte("{\"code\": \"ORDER_NOT_FOUND\", \"message\": \"篮子不存在\"}")
	NOT_AUTHORIZED_ORDER_MSG = []byte("{\"code\": \"NOT_AUTHORIZED_TO_ACCESS_ORDER\",\"message\": \"无权限访问指定的篮子\"}")
	ORDER_PAID_MSG           = []byte("{\"code\": \"ORDER_PAID\",\"message\": \"订单已支付\"}")
	BALANCE_INSUFFICIENT_MSG = []byte("{\"code\": \"BALANCE_INSUFFICIENT\",\"message\": \"余额不足\"}")
)

var (
	server    *http.ServeMux
	kvClient  *kvstore.Client
	rootToken string
)

func InitService(appAddr, kvstoreAddr, userCsv, itemCsv string) {
	kvClient = kvstore.NewClient(kvstoreAddr)

	loadUsersAndItems(userCsv, itemCsv)
	return

	server = http.NewServeMux()
	server.HandleFunc(LOGIN, login)
	server.HandleFunc(QUERY_ITEM, queryFood)
	server.HandleFunc(CREATE_CART, createCart)
	server.HandleFunc(Add_ITEM, addFood)
	server.HandleFunc(SUBMIT_OR_QUERY_ORDER, orderProcess)
	server.HandleFunc(QUERY_ALL_ORDERS, queryAllOrders)
	log.Printf("Start shopping service on %s\n", appAddr)
	defer log.Println("Closed shopping service")
	if err := http.ListenAndServe(appAddr, server); err != nil {
		fmt.Println(err)
	}
}

/**
 * Load user and item data to kvstore.
 */
func loadUsersAndItems(userCsv, itemCsv string) {
	log.Println("Load user and item data to kvstore")
	defer log.Println("Finished data loading")

	kvClient.Put(CART_ID_KEY, "0")

	ItemList = make([]Item, 1, 512)
	ItemList[0] = Item{Id: 0}

	UserMap = make(map[string]UserIdAndPass)

	// read users
	if file, err := os.Open(userCsv); err == nil {
		reader := csv.NewReader(file)
		for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
			userId, _ := strconv.Atoi(strs[0])
			UserMap[strs[1]] = UserIdAndPass{userId, strs[2]}
			kvClient.HSet(BALANCE_KEY, userId2Token(userId), strs[3])
			if userId > MaxUserID {
				MaxUserID = userId
			}
		}
		file.Close()
	} else {
		panic(err.Error())
	}

	rootToken = userId2Token(UserMap["root"].Id)

	// read items
	itemCnt := 0
	cost := int64(0)
	if file, err := os.Open(itemCsv); err == nil {
		reader := csv.NewReader(file)
		for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
			itemCnt++
			itemId, _ := strconv.Atoi(strs[0])
			price, _ := strconv.Atoi(strs[1])
			stock, _ := strconv.Atoi(strs[2])
			ItemList = append(ItemList, Item{Id: itemId, Price: price, Stock: stock})

			start := time.Now()
			kvClient.HSet(ITEMS_KEY, strs[0], strs[2])
			cost += time.Since(start).Nanoseconds()

			if itemId > MaxItemID {
				MaxItemID = itemId
			}
		}

		file.Close()
	} else {
		panic(err.Error())
	}
	fmt.Println(float64(cost) / 1000000000.0)
	fmt.Println(float64(kvstore.HSetCost) / 1000000000.0)
	fmt.Println(float64(kvstore.DailCost) / 1000000000.0)

	// check hget and hset in kvstore
	// if file, err := os.Open(itemCsv); err == nil {
	// 	reader := csv.NewReader(file)
	// 	for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
	// 		itemId, _ := strconv.Atoi(strs[0])
	// 		stock, _ := strconv.Atoi(strs[2])

	// 		if ok, reply := kvClient.HGet(ITEMS_KEY, strs[0]); !ok || reply.Value != strs[2] {
	// 			fmt.Println(itemId, stock)
	// 		}

	// 		if itemId > MaxItemID {
	// 			MaxItemID = itemId
	// 		}
	// 	}

	// 	file.Close()
	// } else {
	// 	panic(err.Error())
	// }
}

func login(writer http.ResponseWriter, req *http.Request) {
	isEmpty, body := checkBodyEmpty(writer, req)
	if isEmpty {
		return
	}
	var user LoginJson
	if err := json.Unmarshal(body, &user); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write(MALFORMED_JSON_MSG)
		return
	}
	userIdAndPass, ok := UserMap[user.Username]
	if !ok || userIdAndPass.Password != user.Password {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(USER_AUTH_FAIL_MSG)
		return
	}
	userId := userIdAndPass.Id
	token := userId2Token(userId)
	kvClient.SAdd("tokens", token)
	okMsg := []byte("{\"user_id\":" + strconv.Itoa(userId) + ",\"username\":\"" + user.Username + "\",\"access_token\":\"" + token + "\"}")
	writer.WriteHeader(http.StatusOK)
	writer.Write(okMsg)
}

func queryFood(writer http.ResponseWriter, req *http.Request) {
	if exist, _ := authorize(writer, req); !exist {
		return
	}
	_, mapReply := kvClient.HGetAll(ITEMS_KEY)
	items := mapReply.Value
	for i := 1; i < len(ItemList); i++ {
		ItemList[i].Stock, _ = strconv.Atoi(items[strconv.Itoa(ItemList[i].Id)])
	}
	itemsJson := make([]byte, 3370)
	itemsJson, _ = json.Marshal(ItemList[1:])
	writer.WriteHeader(http.StatusOK)
	writer.Write(itemsJson)
}

func createCart(writer http.ResponseWriter, req *http.Request) {
	var token string
	exist, token := authorize(writer, req)
	if !exist {
		return
	}

	_, reply := kvClient.Incr(CART_ID_KEY, 1)
	cartIdStr := reply.Value

	cartItemNumKey, _ := getCartKeys(cartIdStr, token)
	_, reply = kvClient.Put(cartItemNumKey, "0")

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("{\"cart_id\": \"" + cartIdStr + "\"}"))
}

func addFood(writer http.ResponseWriter, req *http.Request) {
	var token string
	exist, token := authorize(writer, req)
	if !exist {
		return
	}

	isEmpty, body := checkBodyEmpty(writer, req)
	if isEmpty {
		return
	}

	var item ItemCount
	if err := json.Unmarshal(body, &item); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write(MALFORMED_JSON_MSG)
		return
	}

	if item.ItemId < 1 || item.ItemId > MaxItemID {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write(ITEM_NOT_FOUND_MSG)
		return
	}

	cartIdStr := strings.Split(req.URL.Path, "/")[2]
	cartId, _ := strconv.Atoi(cartIdStr)

	if cartId < 1 {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write(CART_NOT_FOUND_MSG)
		return
	}

	flag := addFoodTrans(cartIdStr, token, strconv.Itoa(item.ItemId), item.Count)

	switch flag {
	case RET_OK:
		{
			writer.WriteHeader(http.StatusNoContent)
		}
	case RET_NOT_FOUND:
		{
			writer.WriteHeader(http.StatusNotFound)
			writer.Write(CART_NOT_FOUND_MSG)
		}
	case RET_NOT_AUTH:
		{
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write(NOT_AUTHORIZED_CART_MSG)
		}
	case RET_ITEM_OUT_OF_LIMIT:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ITEM_OUT_OF_LIMIT_MSG)
		}
	}
	return
}

func orderProcess(writer http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		submitOrder(writer, req)
	} else {
		queryOneOrder(writer, req)
	}
}

func submitOrder(writer http.ResponseWriter, req *http.Request) {
	var token string
	exist, token := authorize(writer, req)
	if !exist {
		return
	}

	isEmpty, body := checkBodyEmpty(writer, req)
	if isEmpty {
		return
	}
	var cartIdJson CartIdJson
	if err := json.Unmarshal(body, &cartIdJson); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write(MALFORMED_JSON_MSG)
		return
	}
	cartIdStr := cartIdJson.IdStr
	cartId, _ := strconv.Atoi(cartIdStr)

	if cartId < 1 {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write(CART_NOT_FOUND_MSG)
		return
	}

	flag := submitOrderTrans(cartIdStr, token)

	switch flag {
	case 0:
		{
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte("{\"id\": \"" + token + "\"}"))
		}
	case 1:
		{
			writer.WriteHeader(http.StatusNotFound)
			writer.Write(CART_NOT_FOUND_MSG)
		}
	case 2:
		{
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write(NOT_AUTHORIZED_CART_MSG)
		}
	case 3:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(CART_EMPTY)
		}
	case 4:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ITEM_OUT_OF_STOCK_MSG)
		}
	case 5:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ORDER_OUT_OF_LIMIT_MSG)
		}
	}
	return
}

func payOrder(writer http.ResponseWriter, req *http.Request) {
	var token string
	exist, token := authorize(writer, req)
	if !exist {
		return
	}

	isEmpty, body := checkBodyEmpty(writer, req)
	if isEmpty {
		return
	}
	var orderIdJson OrderIdJson
	if err := json.Unmarshal(body, &orderIdJson); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write(MALFORMED_JSON_MSG)
		return
	}
	orderIdStr := orderIdJson.IdStr

	flag := payOrderTrans(orderIdStr, token)

	switch flag {
	case RET_OK:
		{
			writer.WriteHeader(http.StatusNoContent)
		}
	case RET_NOT_FOUND:
		{
			writer.WriteHeader(http.StatusNotFound)
			writer.Write(ORDER_NOT_FOUND_MSG)
		}
	case RET_NOT_AUTH:
		{
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write(NOT_AUTHORIZED_ORDER_MSG)
		}
	case RET_ORDER_PAID:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ORDER_PAID_MSG)
		}
	case RET_BALANCE_INSUFFICIENT:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(BALANCE_INSUFFICIENT_MSG)
		}
	}
	return
}

func queryOneOrder(writer http.ResponseWriter, req *http.Request) {
	var token string
	exist, token := authorize(writer, req)
	if !exist {
		return
	}

	var reply kvstore.Reply

	if _, reply = kvClient.HGet(ORDERS_KEY, token); !reply.Flag {
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("[]"))
		return
	}
	hasPaid, cartIdStr, total := parseOrderInfo(reply.Value)
	_, cartContentKey := getCartKeys(cartIdStr, token)

	var mapReply kvstore.MapReply
	_, mapReply = kvClient.HGetAll(cartContentKey)
	itemIdAndCounts := mapReply.Value

	var orders [1]Order
	order := &orders[0]
	itemNum := len(itemIdAndCounts) // it cannot be zero.
	order.HasPaid = hasPaid
	order.IdStr = token
	order.Items = make([]ItemCount, itemNum)
	order.Total = total
	cnt := 0
	for itemIdStr, itemCntStr := range itemIdAndCounts {
		itemId, _ := strconv.Atoi(itemIdStr)
		itemCnt, _ := strconv.Atoi(itemCntStr)
		if itemCnt != 0 {
			order.Items[cnt].ItemId = itemId
			order.Items[cnt].Count = itemCnt
			cnt++
		}
	}

	body, _ := json.Marshal(orders)
	writer.WriteHeader(http.StatusOK)
	writer.Write(body)
}

func queryAllOrders(writer http.ResponseWriter, req *http.Request) {
	var token string
	exist, token := authorize(writer, req)
	if !exist {
		return
	}

	start := time.Now()

	if token != rootToken {
		writer.WriteHeader(http.StatusUnauthorized)
		writer.Write(INVALID_ACCESS_TOKEN_MSG)
		return
	}

	var mapReply kvstore.MapReply
	_, mapReply = kvClient.HGetAll(ORDERS_KEY)
	ordersMap := mapReply.Value
	orders := make([]OrderDetail, len(ordersMap))
	cnt := 0

	for orderIdStr, orderInfo := range ordersMap {
		userToken := orderIdStr
		hasPaid, cartIdStr, total := parseOrderInfo(orderInfo)

		_, cartContentKey := getCartKeys(cartIdStr, userToken)
		_, mapReply = kvClient.HGetAll(cartContentKey)
		itemIdAndCounts := mapReply.Value

		itemNum := len(itemIdAndCounts) // it cannot be zero.
		orders[cnt].IdStr = orderIdStr
		orders[cnt].UserId = token2UserId(userToken)
		orders[cnt].Items = make([]ItemCount, itemNum)
		orders[cnt].Total = total
		orders[cnt].HasPaid = hasPaid

		cnt2 := 0
		for itemIdStr, itemCntStr := range itemIdAndCounts {
			itemId, _ := strconv.Atoi(itemIdStr)
			itemCnt, _ := strconv.Atoi(itemCntStr)
			if itemCnt != 0 {
				orders[cnt].Items[cnt2].ItemId = itemId
				orders[cnt].Items[cnt2].Count = itemCnt
				cnt2++
			}
		}
		cnt++
	}

	body, _ := json.Marshal(orders)
	writer.WriteHeader(http.StatusOK)
	writer.Write(body)
	end := time.Now().Sub(start)
	fmt.Println("queryAllOrders time: ", end.String())
}

// Every action will do authorization except logining.
// @return the flag that indicate whether is authroized or not
func authorize(writer http.ResponseWriter, req *http.Request) (bool, string) {
	req.ParseForm()
	token := req.Form.Get("access_token")
	if token == "" {
		token = req.Header.Get("Access-Token")
	}

	authUserId := token2UserId(token)
	authUserIdStr := strconv.Itoa(authUserId)

	if authUserId < 1 || authUserId > MaxUserID {
		writer.WriteHeader(http.StatusUnauthorized)
		writer.Write(INVALID_ACCESS_TOKEN_MSG)
		return false, ""
	}

	if _, reply := kvClient.SIsMember("tokens", authUserIdStr); !reply.Flag {
		writer.WriteHeader(http.StatusUnauthorized)
		writer.Write(INVALID_ACCESS_TOKEN_MSG)
		return false, ""
	}

	return true, authUserIdStr
}

const PARSE_BUFF_INIT_LEN = 128

func checkBodyEmpty(writer http.ResponseWriter, req *http.Request) (bool, []byte) {
	var parseBuff [PARSE_BUFF_INIT_LEN]byte
	var ptr, totalReadN = 0, 0
	ret := make([]byte, 0, PARSE_BUFF_INIT_LEN/2)

	for readN, _ := req.Body.Read(parseBuff[ptr:]); readN != 0; readN, _ = req.Body.Read(parseBuff[ptr:]) {
		totalReadN += readN
		nextPtr := ptr + readN
		ret = append(ret, parseBuff[ptr:nextPtr]...)
		if nextPtr >= PARSE_BUFF_INIT_LEN {
			ptr = 0
		} else {
			ptr = nextPtr
		}
	}

	if totalReadN == 0 {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write(EMPTY_REQUEST_MSG)
		return true, nil
	} else {
		return false, ret
	}
}

// Retrun the item-num-key and content-key in kvstore.
func getCartKeys(cartIdStr, token string) (cartItemNumKey, cartContentKey string) {
	cartItemNumKey = "cart:" + cartIdStr + ":" + token + ":num"
	cartContentKey = "cart:" + cartIdStr + ":" + token
	return
}

func userId2Token(userId int) string {
	return strconv.Itoa(userId)
}

func token2UserId(token string) int {
	if id, err := strconv.Atoi(token); err == nil {
		return id
	} else {
		panic(err)
	}
}

func composeOrderInfo(hasPaid bool, cartIdStr string, total int) string {
	var info [3]string
	if hasPaid {
		info[0] = ORDER_PAID_FLAG
	} else {
		info[0] = ORDER_UNPAID_FLAG
	}
	info[1] = cartIdStr
	info[2] = strconv.Itoa(total)
	return strings.Join(info[:], ",")
}

func parseOrderInfo(orderInfo string) (hasPaid bool, cartIdStr string, total int) {
	info := strings.Split(orderInfo, ",")
	if info[0] == ORDER_PAID_FLAG {
		hasPaid = true
	} else {
		hasPaid = false
	}
	cartIdStr = info[1]
	total, _ = strconv.Atoi(info[2])
	return
}

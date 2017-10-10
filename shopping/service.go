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
	"sync"
	"time"
)

const (
	LOGIN                 = "/login"
	QUERY_ITEM            = "/items"
	CREATE_CART           = "/carts"
	Add_ITEM              = "/carts/"
	SUBMIT_OR_QUERY_ORDER = "/orders"
	PAY_ORDER             = "/pay"
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

type ShopServer struct {
	server       *http.Server
	handler      *http.Handler
	rootToken    string
	kvClientPool *sync.Pool

	// resident memory
	ItemList  []Item // real item start from index 1
	ItemLock  sync.Mutex
	UserMap   map[string]UserIdAndPass // map[name]password
	MaxItemID int
	MaxUserID int
}

func InitService(appAddr, kvstoreAddr, userCsv, itemCsv string) *ShopServer {
	ss := new(ShopServer)
	ss.kvClientPool = &sync.Pool{
		New: func() interface{} {
			// log.Println("New client")
			return kvstore.NewClient(kvstoreAddr)
		},
	}
	ss.loadUsersAndItems(userCsv, itemCsv)

	handler := http.NewServeMux()
	ss.server = &http.Server{
		Addr:    appAddr,
		Handler: handler,
		// ReadTimeout:    10 * time.Second,
		// WriteTimeout:   10 * time.Second,
		// MaxHeaderBytes: 1 << 20,
	}
	handler.HandleFunc(LOGIN, ss.login)
	handler.HandleFunc(QUERY_ITEM, ss.queryFood)
	handler.HandleFunc(CREATE_CART, ss.createCart)
	handler.HandleFunc(Add_ITEM, ss.addFood)
	handler.HandleFunc(SUBMIT_OR_QUERY_ORDER, ss.orderProcess)
	handler.HandleFunc(PAY_ORDER, ss.payOrder)
	handler.HandleFunc(QUERY_ALL_ORDERS, ss.queryAllOrders)
	log.Printf("Start shopping service on %s\n", appAddr)
	go func() {
		if err := ss.server.ListenAndServe(); err != nil {
			fmt.Println(err)
		}
	}()
	return ss
}

func (ss *ShopServer) Kill() {
	log.Println("Kill the http server")
	if err := ss.server.Close(); err != nil {
		log.Fatal("Http server close error:", err)
	}
}

/**
 * Load user and item data to kvstore.
 */
func (ss *ShopServer) loadUsersAndItems(userCsv, itemCsv string) {
	log.Println("Load user and item data to kvstore")
	defer log.Println("Finished data loading")

	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)

	kvClient.Put(CART_ID_KEY, "0")

	ss.ItemList = make([]Item, 1, 512)
	ss.ItemList[0] = Item{Id: 0}

	ss.UserMap = make(map[string]UserIdAndPass)

	// read users
	if file, err := os.Open(userCsv); err == nil {
		reader := csv.NewReader(file)
		for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
			userId, _ := strconv.Atoi(strs[0])
			ss.UserMap[strs[1]] = UserIdAndPass{userId, strs[2]}
			kvClient.HSet(BALANCE_KEY, userId2Token(userId), strs[3])
			if userId > ss.MaxUserID {
				ss.MaxUserID = userId
			}
		}
		file.Close()
	} else {
		panic(err.Error())
	}

	ss.rootToken = userId2Token(ss.UserMap["root"].Id)

	// read items
	itemCnt := 0
	if file, err := os.Open(itemCsv); err == nil {
		reader := csv.NewReader(file)
		for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
			itemCnt++
			itemId, _ := strconv.Atoi(strs[0])
			price, _ := strconv.Atoi(strs[1])
			stock, _ := strconv.Atoi(strs[2])
			ss.ItemList = append(ss.ItemList, Item{Id: itemId, Price: price, Stock: stock})

			kvClient.HSet(ITEMS_KEY, strs[0], strs[2])

			if itemId > ss.MaxItemID {
				ss.MaxItemID = itemId
			}
		}

		file.Close()
	} else {
		panic(err.Error())
	}

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

func (ss *ShopServer) login(writer http.ResponseWriter, req *http.Request) {

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
	userIdAndPass, ok := ss.UserMap[user.Username]
	if !ok || userIdAndPass.Password != user.Password {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(USER_AUTH_FAIL_MSG)
		return
	}
	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)

	userId := userIdAndPass.Id
	token := userId2Token(userId)
	kvClient.SAdd("tokens", token)
	okMsg := []byte("{\"user_id\":" + strconv.Itoa(userId) + ",\"username\":\"" + user.Username + "\",\"access_token\":\"" + token + "\"}")
	writer.WriteHeader(http.StatusOK)
	writer.Write(okMsg)
	// fmt.Println("login", user, string(okMsg))
}

func (ss *ShopServer) queryFood(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)

	if exist, _ := ss.authorize(writer, req, kvClient); !exist {
		return
	}
	_, mapReply := kvClient.HGetAll(ITEMS_KEY)
	items := mapReply.Value
	// TODO data race
	ss.ItemLock.Lock()
	for i := 1; i < len(ss.ItemList); i++ {
		ss.ItemList[i].Stock, _ = strconv.Atoi(items[strconv.Itoa(ss.ItemList[i].Id)])
	}
	itemsJson := make([]byte, 3370)
	itemsJson, _ = json.Marshal(ss.ItemList[1:])
	ss.ItemLock.Unlock()
	writer.WriteHeader(http.StatusOK)
	writer.Write(itemsJson)
	// fmt.Println("queryFood")
	return
}

func (ss *ShopServer) createCart(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient)
	if !exist {
		return
	}

	_, reply := kvClient.Incr(CART_ID_KEY, 1)
	cartIdStr := reply.Value

	cartItemNumKey, _ := getCartKeys(cartIdStr, token)
	_, reply = kvClient.Put(cartItemNumKey, "0")

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("{\"cart_id\": \"" + cartIdStr + "\"}"))
	// fmt.Println("craeteCart", cartIdStr)
	return
}

func (ss *ShopServer) addFood(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient)
	if !exist {
		return
	}

	isEmpty, body := checkBodyEmpty(writer, req)
	if isEmpty {
		return
	}

	// fmt.Println(string(body))
	var item ItemCount
	if err := json.Unmarshal(body, &item); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write(MALFORMED_JSON_MSG)
		return
	}

	if item.ItemId < 1 || item.ItemId > ss.MaxItemID {
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

	// fmt.Println("addFoodTrans")
	flag := ss.addFoodTrans(cartIdStr, token, strconv.Itoa(item.ItemId), item.Count, kvClient)

	switch flag {
	case RET_OK:
		{
			writer.WriteHeader(http.StatusNoContent)
		}
	case RET_NOT_FOUND:
		{
			// fmt.Println("NOT_FOUND")
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
	// fmt.Println("addItem", flag)
	return
}

func (ss *ShopServer) orderProcess(writer http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		ss.submitOrder(writer, req)
		// fmt.Println("submitOrder")
	} else {
		ss.queryOneOrder(writer, req)
		// fmt.Println("queryOneOrder")
	}
}

func (ss *ShopServer) submitOrder(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient)
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

	flag := ss.submitOrderTrans(cartIdStr, token, kvClient)

	switch flag {
	case 0:
		{
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte("{\"order_id\": \"" + token + "\"}"))
		}
	case 1:
		{
			writer.WriteHeader(http.StatusNotFound)
			writer.Write(CART_NOT_FOUND_MSG)
		}
	case 2:
		{
			fmt.Println(string(NOT_AUTHORIZED_CART_MSG))
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write(NOT_AUTHORIZED_CART_MSG)
		}
	case 3:
		{
			fmt.Println(string(CART_EMPTY))
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(CART_EMPTY)
		}
	case 4:
		{
			fmt.Println(string(ITEM_OUT_OF_STOCK_MSG))
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ITEM_OUT_OF_STOCK_MSG)
		}
	case 5:
		{
			fmt.Println(string(ORDER_OUT_OF_LIMIT_MSG))
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ORDER_OUT_OF_LIMIT_MSG)
		}
	}
	// fmt.Println("submitOrder", flag)
	return
}

func (ss *ShopServer) payOrder(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)
	var token string

	exist, token := ss.authorize(writer, req, kvClient)
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

	flag := ss.payOrderTrans(orderIdStr, token, kvClient)

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
	// fmt.Println("payOrder", flag)
	return
}

func (ss *ShopServer) queryOneOrder(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient)
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
	return
}

func (ss *ShopServer) queryAllOrders(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*kvstore.Client)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient)
	if !exist {
		return
	}

	start := time.Now()

	if token != ss.rootToken {
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
	return
}

// Every action will do authorization except logining.
// @return the flag that indicate whether is authroized or not
func (ss *ShopServer) authorize(writer http.ResponseWriter, req *http.Request, kvClient *kvstore.Client) (bool, string) {
	req.ParseForm()
	token := req.Form.Get("access_token")
	if token == "" {
		token = req.Header.Get("Access-Token")
	}

	authUserId := token2UserId(token)
	authUserIdStr := strconv.Itoa(authUserId)

	if authUserId < 1 || authUserId > ss.MaxUserID {
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

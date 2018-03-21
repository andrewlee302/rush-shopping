package shopping

// Rush-shopping service.
//
// We assume the followings:
// * The IDs of items are increasing from 1 continuously.
// * The ID of the (root) administrator user is 0.
// * The IDs of normal users are increasing from 1 continuously.
// * CartID is auto-increased from 1.
//
// The data format in KV-Store could be referred in
// shop_kvformat.md.

import (
	"distributed-system/twopc"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"rush-shopping/kv"
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

// Keys of kvstore
const (
	TokenKeyPrefix      = "token:"
	OrderKeyPrefix      = "order:"
	ItemsStockKeyPrefix = "items_stock:"
	ItemsPriceKeyPrefix = "items_price:"
	BalanceKeyPrefix    = "balance:"

	CartIDMaxKey = "cartID"
	ItemsSizeKey = "items_size"
)

const (
	OrderPaidFlag   = "P" // have been paid
	OrderUnpaidFlag = "W" // wait to be paid
)

const RootUserID = 0

var RootUserToken = userID2Token(RootUserID)

// Trans status
const (
	TxnOK       = 0
	TxnNotFound = 1 << (iota - 1) // iota == 1
	TxnNotAuth
	TxnCartEmpyt
	TxnOutOfStock      // out of stock
	TxnItemOutOfLimit  // most 3 items
	TxnOrderOutOfLimit // most one order for one person
	TxnOrderPaid
	TxnBalanceInsufficient
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
	server    *http.Server
	handler   *http.Handler
	rootToken string

	coordClient  *CoordClient
	kvClientPool *sync.Pool

	// resident memory
	ItemList  []Item // real item start from index 1
	ItemLock  sync.Mutex
	UserMap   map[string]UserIDAndPass // map[name]password
	MaxItemID int                      // The same with the number of types of items.
	MaxUserID int                      // The same with the number of normal users.
}

func InitService(appAddr, coordAddr, userCsv, itemCsv string,
	kvstoreAddrs []string, keyHashFunc twopc.KeyHashFunc) *ShopServer {
	// fmt.Println(TxnOK,
	// 	TxnNotFound,
	// 	TxnNotAuth,
	// 	TxnCartEmpyt,
	// 	TxnOutOfStock,
	// 	TxnItemOutOfLimit,
	// 	TxnOrderOutOfLimit,
	// 	TxnOrderPaid,
	// 	TxnBalanceInsufficient)
	ss := new(ShopServer)
	ss.coordClient = NewCoordClient(coordAddr)
	ss.kvClientPool = &sync.Pool{
		New: func() interface{} {
			// log.Println("New client")
			return NewShardsClient(kvstoreAddrs, keyHashFunc)
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
	handler.HandleFunc(QUERY_ITEM, ss.queryItem)
	handler.HandleFunc(CREATE_CART, ss.createCart)
	handler.HandleFunc(Add_ITEM, ss.addItem)
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

	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)

	kvClient.Put(CartIDMaxKey, "0")

	ss.ItemList = make([]Item, 1, 512)
	ss.ItemList[0] = Item{ID: 0}

	ss.UserMap = make(map[string]UserIDAndPass)

	// read users
	if file, err := os.Open(userCsv); err == nil {
		reader := csv.NewReader(file)
		for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
			userID, _ := strconv.Atoi(strs[0])
			ss.UserMap[strs[1]] = UserIDAndPass{userID, strs[2]}
			userToken := userID2Token(userID)
			kvClient.Put(BalanceKeyPrefix+userToken, strs[3])
			if userID > ss.MaxUserID {
				ss.MaxUserID = userID
			}
		}
		file.Close()
	} else {
		panic(err.Error())
	}

	ss.rootToken = userID2Token(ss.UserMap["root"].ID)

	// read items
	itemCnt := 0
	if file, err := os.Open(itemCsv); err == nil {
		reader := csv.NewReader(file)
		for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
			itemCnt++
			itemID, _ := strconv.Atoi(strs[0])
			price, _ := strconv.Atoi(strs[1])
			stock, _ := strconv.Atoi(strs[2])
			ss.ItemList = append(ss.ItemList, Item{ID: itemID, Price: price, Stock: stock})

			kvClient.Put(ItemsPriceKeyPrefix+strs[0], strs[1])
			kvClient.Put(ItemsStockKeyPrefix+strs[0], strs[2])

			if itemID > ss.MaxItemID {
				ss.MaxItemID = itemID
			}
		}
		kvClient.Put(ItemsSizeKey, strconv.Itoa(itemCnt))

		file.Close()
	} else {
		panic(err.Error())
	}
	ss.coordClient.LoadItemList(itemCnt)
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
	userIDAndPass, ok := ss.UserMap[user.Username]
	if !ok || userIDAndPass.Password != user.Password {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(USER_AUTH_FAIL_MSG)
		return
	}
	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)

	userID := userIDAndPass.ID
	token := userID2Token(userID)
	kvClient.Put(TokenKeyPrefix+token, "1")
	okMsg := []byte("{\"user_id\":" + strconv.Itoa(userID) + ",\"username\":\"" + user.Username + "\",\"access_token\":\"" + token + "\"}")
	writer.WriteHeader(http.StatusOK)
	writer.Write(okMsg)
}

// TODO consistency tradeoff for perf
func (ss *ShopServer) queryItem(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)

	if exist, _ := ss.authorize(writer, req, kvClient, false); !exist {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(ss.ItemList) - 1)
	// TODO data race
	ss.ItemLock.Lock()
	for i := 1; i < len(ss.ItemList); i++ {
		go func(i int) {
			_, reply := kvClient.Get(ItemsPriceKeyPrefix + strconv.Itoa(ss.ItemList[i].ID))
			ss.ItemList[i].Stock, _ = strconv.Atoi(reply.Value)
			wg.Done()
		}(i)
	}
	wg.Wait()
	itemsJson := make([]byte, 3370)
	itemsJson, _ = json.Marshal(ss.ItemList[1:])
	ss.ItemLock.Unlock()
	writer.WriteHeader(http.StatusOK)
	writer.Write(itemsJson)
	return
}

func (ss *ShopServer) createCart(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient, false)
	if !exist {
		return
	}

	_, reply := kvClient.Incr(CartIDMaxKey, 1)
	cartIDStr := reply.Value

	cartItemNumKey, _ := getCartKeys(cartIDStr, token)
	_, reply = kvClient.Put(cartItemNumKey, "0")

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("{\"cart_id\": \"" + cartIDStr + "\"}"))
	return
}

func (ss *ShopServer) addItem(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient, false)
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

	if item.ItemID < 1 || item.ItemID > ss.MaxItemID {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write(ITEM_NOT_FOUND_MSG)
		return
	}

	cartIDStr := strings.Split(req.URL.Path, "/")[2]
	cartID, _ := strconv.Atoi(cartIDStr)

	if cartID < 1 {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write(CART_NOT_FOUND_MSG)
		return
	}

	// fmt.Println("addItemTrans")
	_, txnID := ss.coordClient.StartAddItemTxn(cartIDStr, token, item.ItemID, item.Count)
	errCode := ss.coordClient.SyncTxn(txnID)
	flag := normalizeErrCode(errCode)
	fmt.Println("addItem", cartIDStr, token, item.ItemID, item.Count, flag)

	switch flag {
	case TxnOK:
		{
			writer.WriteHeader(http.StatusNoContent)
		}
	case TxnNotFound:
		{
			writer.WriteHeader(http.StatusNotFound)
			writer.Write(CART_NOT_FOUND_MSG)
		}
	case TxnNotAuth:
		{
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write(NOT_AUTHORIZED_CART_MSG)
		}
	case TxnItemOutOfLimit:
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
	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient, false)
	if !exist {
		return
	}

	isEmpty, body := checkBodyEmpty(writer, req)
	if isEmpty {
		return
	}
	var cartIDJson CartIDJson
	if err := json.Unmarshal(body, &cartIDJson); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write(MALFORMED_JSON_MSG)
		return
	}
	cartIDStr := cartIDJson.IDStr
	cartID, _ := strconv.Atoi(cartIDStr)

	if cartID < 1 {
		writer.WriteHeader(http.StatusNotFound)
		writer.Write(CART_NOT_FOUND_MSG)
		return
	}

	_, txnID := ss.coordClient.StartSubmitOrderTxn(cartIDStr, token)
	errCode := ss.coordClient.SyncTxn(txnID)
	flag := normalizeErrCode(errCode)
	fmt.Println("submit", cartIDStr, token, flag)

	switch flag {
	case TxnOK:
		{
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte("{\"order_id\": \"" + token + "\"}"))
		}
	case TxnNotFound:
		{
			writer.WriteHeader(http.StatusNotFound)
			writer.Write(CART_NOT_FOUND_MSG)
		}
	case TxnNotAuth:
		{
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write(NOT_AUTHORIZED_CART_MSG)
		}
	case TxnCartEmpyt:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(CART_EMPTY)
		}
	case TxnOutOfStock:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ITEM_OUT_OF_STOCK_MSG)
		}
	case TxnOrderOutOfLimit:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ORDER_OUT_OF_LIMIT_MSG)
		}
	}
	// fmt.Println("submitOrder", flag)
	return
}

func (ss *ShopServer) payOrder(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)
	var token string

	exist, token := ss.authorize(writer, req, kvClient, false)
	if !exist {
		return
	}

	isEmpty, body := checkBodyEmpty(writer, req)
	if isEmpty {
		return
	}
	var orderIDJson OrderIDJson
	if err := json.Unmarshal(body, &orderIDJson); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write(MALFORMED_JSON_MSG)
		return
	}
	orderIDStr := orderIDJson.IDStr

	_, txnID := ss.coordClient.StartSubmitOrderTxn(orderIDStr, token)
	errCode := ss.coordClient.SyncTxn(txnID)
	flag := normalizeErrCode(errCode)
	fmt.Println("payOrder", orderIDStr, token, flag)

	switch flag {
	case TxnOK:
		{
			writer.WriteHeader(http.StatusNoContent)
		}
	case TxnNotFound:
		{
			writer.WriteHeader(http.StatusNotFound)
			writer.Write(ORDER_NOT_FOUND_MSG)
		}
	case TxnNotAuth:
		{
			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write(NOT_AUTHORIZED_ORDER_MSG)
		}
	case TxnOrderPaid:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(ORDER_PAID_MSG)
		}
	case TxnBalanceInsufficient:
		{
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(BALANCE_INSUFFICIENT_MSG)
		}
	}
	// fmt.Println("payOrder", flag)
	return
}

func (ss *ShopServer) queryOneOrder(writer http.ResponseWriter, req *http.Request) {
	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)

	var token string
	exist, token := ss.authorize(writer, req, kvClient, false)
	if !exist {
		return
	}

	var reply kv.Reply

	if _, reply = kvClient.Get(OrderKeyPrefix + token); !reply.Flag {
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("[]"))
		return
	}
	hasPaid, cartIDStr, total := parseOrderInfo(reply.Value)
	_, cartDetailKey := getCartKeys(cartIDStr, token)

	_, reply = kvClient.Get(cartDetailKey)
	itemIDAndCounts := parseCartDetail(reply.Value)

	var orders [1]Order
	order := &orders[0]
	itemNum := len(itemIDAndCounts) // it cannot be zero.
	order.HasPaid = hasPaid
	order.IDStr = token
	order.Items = make([]ItemCount, itemNum)
	order.Total = total
	cnt := 0
	for itemID, itemCnt := range itemIDAndCounts {
		if itemCnt != 0 {
			order.Items[cnt].ItemID = itemID
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
	kvClient := ss.kvClientPool.Get().(*ShardsClient)
	defer ss.kvClientPool.Put(kvClient)

	exist, _ := ss.authorize(writer, req, kvClient, true)
	if !exist {
		return
	}

	start := time.Now()

	orders := make([]OrderDetail, 0, ss.MaxUserID)

	for userID := 1; userID < ss.MaxUserID; userID++ {
		userToken := userID2Token(userID)
		_, reply := kvClient.Get(OrderKeyPrefix + userToken)
		if !reply.Flag {
			continue
		}
		orderInfo := reply.Value
		hasPaid, cartIDStr, total := parseOrderInfo(orderInfo)

		_, cartDetailKey := getCartKeys(cartIDStr, userToken)
		_, reply = kvClient.Get(cartDetailKey)
		itemIDAndCounts := parseCartDetail(reply.Value)

		itemNum := len(itemIDAndCounts) // it cannot be zero.
		orderDetail := OrderDetail{UserID: userID, Order: Order{IDStr: userToken, Items: make([]ItemCount, 0, itemNum), Total: total, HasPaid: hasPaid}}

		for itemID, itemCnt := range itemIDAndCounts {
			if itemCnt != 0 {
				orderDetail.Items = append(orderDetail.Items, ItemCount{ItemID: itemID, Count: itemCnt})
			}
		}
		orders = append(orders, orderDetail)
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
func (ss *ShopServer) authorize(writer http.ResponseWriter, req *http.Request, kvClient *ShardsClient, isRoot bool) (bool, string) {
	valid := true
	var authUserID int
	var authUserIDStr string
	req.ParseForm()
	token := req.Form.Get("access_token")
	if token == "" {
		token = req.Header.Get("Access-Token")
	}

	if token == "" {
		valid = false
	} else {
		authUserID = token2UserID(token)
		authUserIDStr = strconv.Itoa(authUserID)

		if isRoot && authUserIDStr != ss.rootToken || !isRoot && (authUserID < 1 || authUserID > ss.MaxUserID) {
			valid = false
		} else {
			if _, reply := kvClient.Get(TokenKeyPrefix + authUserIDStr); !reply.Flag {
				valid = false
			}
		}
	}

	if !valid {
		writer.WriteHeader(http.StatusUnauthorized)
		writer.Write(INVALID_ACCESS_TOKEN_MSG)
		return false, ""
	}
	return true, authUserIDStr
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
	}
	return false, ret
}

func normalizeErrCode(errCode int) int {
	fmt.Println("normalizeErrCode", errCode)
	if errCode <= 0 {
		return errCode
	}
	cnt := 0
	for errCode&0x01 != 0x01 {
		errCode = errCode >> 1
		cnt++
	}
	return 1 << uint(cnt)
}

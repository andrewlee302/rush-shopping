package shopping

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"rush-shopping/kvstore"
	"strconv"
	"sync"
	"testing"
	"time"
)

//----------------------------------
// Stress Abstracts
//----------------------------------
type Worker struct {
	// r   *Reporter
	ctx struct {
		hosts []string
		port  int
	}
	t        *testing.T
	userChan <-chan User
}

type SessionContext struct {
	c          *http.Client
	w          *Worker
	user       User
	cartIdStr  string
	orderIdStr string
}

type Reporter struct {
	orderMade       chan bool
	orderCost       chan time.Duration
	payMade         chan bool
	payCost         chan time.Duration
	requestSent     chan bool
	userCurr        chan User
	numOrders       int
	cocurrency      int
	nOrderOk        int
	nOrderErr       int
	nOrderTotal     int
	nOrderPerSec    []int
	orderCosts      []time.Duration
	nPayOk          int
	nPayErr         int
	nPayTotal       int
	nPayPerSec      []int
	payCosts        []time.Duration
	nRequestOk      int
	nRequestErr     int
	nRequestTotal   int
	nRequestPerSec  []int
	timeStampPerSec []int
	startAt         time.Time
	elapsed         time.Duration
}

//----------------------------------
// Entity Abstracts
//----------------------------------
type User struct {
	Id          int
	Username    string
	Password    string
	AccessToken string
}

//----------------------------------
// Request JSON Bindings
//----------------------------------
type RequestLogin struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RequestCartAddItem struct {
	ItemId int `json:"item_id"`
	Count  int `json:"count"`
}

type RequestMakeOrder struct {
	CartIdStr string `json:"cart_id"`
}

type RequestPayOrder struct {
	OrderIdStr string `json:"order_id"`
}

//----------------------------------
// Response JSON Bindings
//----------------------------------
type ResponseLogin struct {
	UserId      int    `json:"user_id"`
	Username    string `json:"username"`
	AccessToken string `json:"access_token"`
}

type ResponseGetItems []Item

type ResponseCreateCart struct {
	CartIdStr string `json:"cart_id"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

type ResponseCartAddItem struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ResponseMakeOrder struct {
	OrderIdStr string `json:"order_id"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

type ResponsePayOrder struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type CheckBox struct {
	user          User
	value         int
	expectedValue int
}

//----------------------------------
// Global Variables
//----------------------------------
var (
	users           = make([]User, 1, 512) // users[0] is root
	items           = make([]Item, 1, 512) // items[0] is useless
	isDebugMode     = false
	isReportToRedis = false
	checkChan       = make(chan *CheckBox)
)

// parse users and items
func parseUsersAndItems(userCsv, itemCsv string) {
	// read users
	if file, err := os.Open(userCsv); err == nil {
		reader := csv.NewReader(file)
		for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
			userId, _ := strconv.Atoi(strs[0])
			user := User{Id: userId, Username: strs[1], Password: strs[2]}
			if userId == 0 {
				// root user
				users[0] = user
			} else {
				users = append(users, user)
			}
		}
		file.Close()
	} else {
		panic(err.Error())
	}

	// read items
	itemCnt := 0
	if file, err := os.Open(itemCsv); err == nil {
		reader := csv.NewReader(file)
		for strs, err := reader.Read(); err == nil; strs, err = reader.Read() {
			itemCnt++
			itemId, _ := strconv.Atoi(strs[0])
			price, _ := strconv.Atoi(strs[1])
			stock, _ := strconv.Atoi(strs[2])
			item := Item{Id: itemId, Price: price, Stock: stock}
			items = append(items, item)
		}
		file.Close()
	} else {
		panic(err.Error())
	}
}

//----------------------------------
// Worker
//----------------------------------
// func NewWorker(hosts []string, port int, r *Reporter) *Worker {
func NewWorker(t *testing.T, userChan <-chan User, hosts []string, port int) *Worker {
	w := &Worker{}
	// w.r = r
	w.userChan = userChan
	w.t = t
	w.ctx.hosts = hosts
	w.ctx.port = port
	return w
}

func check(t *testing.T, user User, value, expected int) bool {
	if value != expected {
		t.Fatalf("User: %v, wrong reply %v; expected %v", user.Username, errStr(value), errStr(expected))
		return false
	}
	return true
}

func (w *Worker) Work() {
	ctx := &SessionContext{}
	ctx.w = w
	t := &http.Transport{}
	ctx.c = &http.Client{
		Timeout:   3 * time.Second,
		Transport: t,
	}
	for {
		t.CloseIdleConnections()
		// startAt := time.Now()
		var ok bool
		ctx.user, ok = <-w.userChan
		if !ok {
			break
		}
		if rand.Float32() < 1 {
			// must execute here
			checkChan <- &CheckBox{user: ctx.user, value: ctx.PayOrderCycle(), expectedValue: REQ_OK}
			// w.r.payMade <- ctx.PayOrderCycle()
			// w.r.payCost <- time.Since(startAt)
		} else {
			checkChan <- &CheckBox{user: ctx.user, value: ctx.MakeOrderCycle(), expectedValue: REQ_OK}
			// w.r.orderMade <- ctx.MakeOrderCycle()
			// w.r.orderCost <- time.Since(startAt)
		}
	}
}

//----------------------------------
// Request Utils
//----------------------------------
// Build url with path and parameters.
func (w *Worker) Url(path string, params url.Values) string {
	// random choice one host for load balance
	i := rand.Intn(len(w.ctx.hosts))
	host := w.ctx.hosts[i]
	s := fmt.Sprintf("http://%s:%d%s", host, w.ctx.port, path)
	if params == nil {
		return s
	}
	p := params.Encode()
	return fmt.Sprintf("%s?%s", s, p)
}

// Get json from uri.
func (w *Worker) Get(c *http.Client, url string, bind interface{}) (int, error) {
	r, err := c.Get(url)
	if err != nil {
		if r != nil {
			ioutil.ReadAll(r.Body)
			r.Body.Close()
		}
		return 0, err
	}
	defer r.Body.Close()
	err = json.NewDecoder(r.Body).Decode(bind)
	if bind == nil {
		return r.StatusCode, nil
	}
	return r.StatusCode, err
}

// Post json to uri and get json response.
func (w *Worker) Post(c *http.Client, url string, data interface{}, bind interface{}) (int, error) {
	var body io.Reader
	if data != nil {
		bs, err := json.Marshal(data)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(bs)
	}
	res, err := c.Post(url, "application/json", body)
	if err != nil {
		if res != nil {
			ioutil.ReadAll(res.Body)
			res.Body.Close()
		}
		return 0, err
	}
	defer res.Body.Close()
	err = json.NewDecoder(res.Body).Decode(bind)
	if res.StatusCode == http.StatusNoContent || bind == nil {
		return res.StatusCode, nil
	}
	return res.StatusCode, err
}

// Patch url with json.
func (w *Worker) Patch(c *http.Client, url string, data interface{}, bind interface{}) (int, error) {
	bs, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest("PATCH", url, bytes.NewReader(bs))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.Do(req)
	if err != nil {
		if res != nil {
			ioutil.ReadAll(res.Body)
			res.Body.Close()
		}
		return 0, err
	}
	defer res.Body.Close()
	err = json.NewDecoder(res.Body).Decode(bind)
	if res.StatusCode == http.StatusNoContent || bind == nil {
		return res.StatusCode, nil
	}
	return res.StatusCode, err
}

//----------------------------------
// Statstics Reporter
//----------------------------------
// Create reporter
func NewReporter(numOrders int, cocurrency int) *Reporter {
	return &Reporter{
		make(chan bool, cocurrency),
		make(chan time.Duration, cocurrency),
		make(chan bool, cocurrency),
		make(chan time.Duration, cocurrency),
		make(chan bool, cocurrency),
		make(chan User, cocurrency),
		numOrders,
		cocurrency,
		0, // order
		0,
		0,
		make([]int, 0),
		make([]time.Duration, 0),
		0, // pay
		0,
		0,
		make([]int, 0),
		make([]time.Duration, 0),
		0,
		0,
		0,
		make([]int, 0),
		make([]int, 0),
		time.Now(),
		0,
	}
}

// Start reporter
func (r *Reporter) Start() {
	r.startAt = time.Now()
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			nOrderOk := r.nOrderOk
			nRequestOk := r.nRequestOk
			<-t.C
			nOrderPerSec := r.nOrderOk - nOrderOk
			r.nOrderPerSec = append(r.nOrderPerSec, nOrderPerSec)
			nRequestPerSec := r.nRequestOk - nRequestOk
			r.nRequestPerSec = append(r.nRequestPerSec, nRequestPerSec)
			r.timeStampPerSec = append(r.timeStampPerSec, time.Now().Second())
			fmt.Printf("Finished orders: %d\n", nOrderPerSec)
		}
	}()
	go func() {
		for {
			select {
			case orderMade := <-r.orderMade:
				{
					orderCost := <-r.orderCost
					if orderMade {
						r.nOrderOk++
						r.orderCosts = append(r.orderCosts, orderCost)
					} else {
						r.nOrderErr++
					}
					r.nOrderTotal++
				}
			case payMade := <-r.payMade:
				{
					payCost := <-r.payCost
					if payMade {
						r.nPayOk++
						r.payCosts = append(r.payCosts, payCost)
					} else {
						r.nPayErr++
					}
				}
				r.nPayTotal++
			}
			if r.nOrderTotal+r.nPayTotal >= r.numOrders {
				r.Stop()
			}
		}
	}()
	go func() {
		for {
			requestSent := <-r.requestSent
			if requestSent {
				r.nRequestOk = r.nRequestOk + 1
			} else {
				r.nRequestErr = r.nRequestErr + 1
			}
			r.nRequestTotal = r.nRequestTotal + 1
		}
	}()
	for i := 1; i < len(users); i++ {
		r.userCurr <- users[i]
	}
	timeout := time.After(3 * time.Second)
	if r.nOrderTotal+r.nPayTotal < r.numOrders {
		select {
		case <-timeout:
			r.Stop()
			return
		}
	}
	r.Stop()
}

// Stop the reporter and exit full process.
func (r *Reporter) Stop() {
	r.elapsed = time.Since(r.startAt)
	// r.Report()
}

//----------------------------------
//  Order Handle Utils
//----------------------------------
// Random choice a item. Dont TUCAO this function,
// it works and best O(1).
func GetRandItem() Item {
	idx := rand.Intn(len(items) - 1)
	item := items[idx+1]
	return item
}

//----------------------------------
//  Work Job SessionContext
//----------------------------------
func (ctx *SessionContext) UrlWithToken(path string) string {
	user := ctx.user
	params := url.Values{}
	params.Add("access_token", user.AccessToken)
	return ctx.w.Url(path, params)
}

const (
	REQ_OK          = iota
	LOGIN_ERR       = 1 << iota
	GET_ITEMS_ERR   = 1 << iota
	CREATE_CART_ERR = 1 << iota
	CART_ADD_ERR    = 1 << iota
	MAKE_ORDER_ERR  = 1 << iota
	PAY_ORDER_ERR   = 1 << iota
)

func errStr(err int) string {
	switch err {
	case LOGIN_ERR:
		return "LOGIN_ERR"
	case GET_ITEMS_ERR:
		return "GET_ITEMS_ERR"
	case CREATE_CART_ERR:
		return "CREATE_CART_ERR"
	case CART_ADD_ERR:
		return "CART_ADD_ERR"
	case MAKE_ORDER_ERR:
		return "MAKE_ORDER_ERR"
	case PAY_ORDER_ERR:
		return "PAY_ORDER_ERR"
	default:
		return "REQ_OK"
	}
}

func (ctx *SessionContext) Login() int {
	user := ctx.user
	data := &RequestLogin{user.Username, user.Password}
	body := &ResponseLogin{}
	url := ctx.w.Url("/login", nil)
	statusCode, err := ctx.w.Post(ctx.c, url, data, body)
	if err != nil {
		if isDebugMode {
			fmt.Printf("Request login error: %v\n", err)
		}
		// ctx.w.r.requestSent <- false
		return LOGIN_ERR
	}
	if statusCode == http.StatusOK {
		ctx.user.AccessToken = body.AccessToken
		// ctx.w.r.requestSent <- true
		return REQ_OK
	}
	// ctx.w.r.requestSent <- false
	return LOGIN_ERR
}

// how to judge the function and perf of the following method?
func (ctx *SessionContext) GetItems() int {
	// body := &ResponseGetItems{}
	url := ctx.UrlWithToken("/items")
	statusCode, err := ctx.w.Get(ctx.c, url, nil)
	if err != nil {
		if isDebugMode {
			fmt.Printf("Request get items error: %v\n", err)
		}
		// ctx.w.r.requestSent <- false
		return GET_ITEMS_ERR
	}
	if statusCode == http.StatusOK {
		// ctx.w.r.requestSent <- true
		return REQ_OK
	}
	// ctx.w.r.requestSent <- false
	return GET_ITEMS_ERR
}

func (ctx *SessionContext) CreateCart() int {
	body := &ResponseCreateCart{}
	url := ctx.UrlWithToken("/carts")
	statusCode, err := ctx.w.Post(ctx.c, url, nil, body)
	if err != nil {
		if isDebugMode {
			fmt.Printf("Request create carts error: %v\n", err)
		}
		// ctx.w.r.requestSent <- false
		return CREATE_CART_ERR
	}
	if statusCode == http.StatusOK {
		ctx.cartIdStr = body.CartIdStr
		// ctx.w.r.requestSent <- true
		return REQ_OK
	}
	// ctx.w.r.requestSent <- false
	return CREATE_CART_ERR
}

func (ctx *SessionContext) CartAddItem() int {
	path := fmt.Sprintf("/carts/%s", ctx.cartIdStr)
	url := ctx.UrlWithToken(path)
	item := GetRandItem()

	data := &RequestCartAddItem{item.Id, 1}
	body := &ResponseCartAddItem{}
	statusCode, err := ctx.w.Patch(ctx.c, url, data, body)
	if err != nil {
		if isDebugMode {
			fmt.Printf("Request error cart add item error: %v\n", err)
		}
		// ctx.w.r.requestSent <- false
		return CART_ADD_ERR
	}
	if statusCode == http.StatusNoContent {
		// ctx.w.r.requestSent <- true
		return REQ_OK
	}
	// ctx.w.r.requestSent <- false
	return CART_ADD_ERR
}

func (ctx *SessionContext) MakeOrder() int {
	data := &RequestMakeOrder{ctx.cartIdStr}
	body := &ResponseMakeOrder{}
	url := ctx.UrlWithToken("/orders")
	statusCode, err := ctx.w.Post(ctx.c, url, data, body)
	if err != nil {
		if isDebugMode {
			fmt.Printf("Request make order error: %v\n", err)
		}
		// ctx.w.r.requestSent <- false
		return MAKE_ORDER_ERR
	}
	if statusCode == http.StatusOK {
		// ctx.w.r.requestSent <- true
		ctx.orderIdStr = body.OrderIdStr
		// fmt.Println("orderId", body.OrderIdStr)
		return REQ_OK
	}
	// ctx.w.r.requestSent <- false
	return MAKE_ORDER_ERR
}

func (ctx *SessionContext) PayOrder() int {
	data := &RequestPayOrder{ctx.orderIdStr}
	body := &ResponsePayOrder{}
	url := ctx.UrlWithToken("/pay")
	statusCode, err := ctx.w.Post(ctx.c, url, data, body)
	if err != nil {
		if isDebugMode {
			fmt.Printf("Request pay order error: %v\n", err)
		}
		// ctx.w.r.requestSent <- false
		return PAY_ORDER_ERR
	}
	if statusCode == http.StatusNoContent {
		// ctx.w.r.requestSent <- true
		return REQ_OK
	}
	// ctx.w.r.requestSent <- false
	return PAY_ORDER_ERR
}

// Include all activities except order paying.
func (ctx *SessionContext) MakeOrderCycle() int {
	if ctx.Login() != REQ_OK {
		return LOGIN_ERR
	} else if ctx.GetItems() != REQ_OK {
		return GET_ITEMS_ERR
	} else if ctx.CreateCart() != REQ_OK {
		return CREATE_CART_ERR
	}
	count := rand.Intn(3) + 1
	for i := 0; i < count; i++ {
		if ctx.CartAddItem() != REQ_OK {
			return CART_ADD_ERR
		}
	}
	return ctx.MakeOrder()
}

// Include all activities.
func (ctx *SessionContext) PayOrderCycle() int {
	if err := ctx.MakeOrderCycle(); err != REQ_OK {
		return err
	}
	err := ctx.PayOrder()
	return err
}

const (
	APP_ADDR     = "localhost:10001"
	KVSTORE_ADDR = "localhost:10002"
	USER_CSV     = "../data/users.csv"
	ITEM_CSV     = "../data/items.csv"
)

var hosts []string = []string{"localhost"}
var port int = 10001
var ts *kvstore.TinyStore
var ss *ShopServer

func startService(appAddr, kvstoreAddr, userCsv, itemCsv string) {
	ts = kvstore.StartTinyStore(kvstoreAddr)
	ss = InitService(appAddr, kvstoreAddr, userCsv, itemCsv)
}

func cleanup() {
	ts.Kill()
	ss.Kill()
}

func checkConstraints() bool {
}

func TestBasic(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	startService(APP_ADDR, KVSTORE_ADDR, USER_CSV, ITEM_CSV)
	defer cleanup()

	// Load users/foods and work for test
	parseUsersAndItems(USER_CSV, ITEM_CSV)

	concurrency := 1

	fmt.Printf("Test: serial requests...\n")
	userChan := make(chan User, concurrency)
	go func() {
		for i := 1; i < len(users); i++ {
			userChan <- users[i]
		}
		close(userChan)
	}()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			w := NewWorker(t, userChan, hosts, port)
			w.Work()
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(checkChan)
	}()

	for cb := range checkChan {
		check(t, cb.user, cb.value, cb.expectedValue)
	}
	fmt.Printf("  ... Passed\n")
}

func TestConcurrent(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	startService(APP_ADDR, KVSTORE_ADDR, USER_CSV, ITEM_CSV)
	defer cleanup()

	// Load users/foods and work for test
	parseUsersAndItems(USER_CSV, ITEM_CSV)

	concurrency := runtime.GOMAXPROCS(0)

	fmt.Printf("Test: concurrent requests...\n")
	userChan := make(chan User, concurrency)
	go func() {
		for i := 1; i < len(users); i++ {
			userChan <- users[i]
		}
		close(userChan)
	}()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			w := NewWorker(t, userChan, hosts, port)
			w.Work()
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(checkChan)
	}()

	for cb := range checkChan {
		check(t, cb.user, cb.value, cb.expectedValue)
	}
	fmt.Printf("  ... Passed\n")
}

/*

APP_HOST=0.0.0.0
APP_PORT=8080

USER_CSV="data/users.csv"
ITEM_CSV="data/items.csv"

KVSTORE_HOST=localhost
KVSTORE_PORT=6379

GOPATH=
JAVA_HOME=

*/

// go run  main.go service.go type.go

package main

import (
	"distributed-system/twopc"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"rush-shopping/shopping"
	"strconv"
	"strings"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	appHost := os.Getenv("APP_HOST")
	appPort := os.Getenv("APP_PORT")
	if appHost == "" {
		appHost = "localhost"
	}
	if appPort == "" {
		appPort = "10000"
	}
	appAddr := fmt.Sprintf("%s:%s", appHost, appPort)

	coordHost := os.Getenv("COORD_HOST")
	coordPort := os.Getenv("COORD_PORT")
	if coordHost == "" {
		coordHost = "localhost"
	}
	if coordPort == "" {
		coordPort = "9999"
	}
	coordAddr := fmt.Sprintf("%s:%s", coordHost, coordPort)

	kvstoreHost := os.Getenv("KVSTORE_HOST")
	kvstorePorts := strings.Split(os.Getenv("KVSTORE_PORTS"), ",")
	var kvstoreAddrs []string
	for _, port := range kvstorePorts {
		kvstoreAddrs = append(kvstoreAddrs, fmt.Sprintf("%s:%s",
			kvstoreHost, port))
	}

	userCsv := os.Getenv("USER_CSV")
	itemCsv := os.Getenv("ITEM_CSV")

	var timeoutMs int64
	timeoutMsStr := os.Getenv("TIMEOUT_MS")
	if timeoutMsStr == "" {
		timeoutMs = 1000
	}
	if timeout, err := strconv.Atoi(timeoutMsStr); err != nil {
		timeoutMs = 1000
	} else {
		timeoutMs = int64(timeout)
	}

	keyHashFunc := twopc.DefaultKeyHashFunc

	kvServices := make([]*shopping.ShoppingTxnKVStoreService,
		len(kvstoreAddrs))
	for i, ppt := range kvstoreAddrs {
		kvServices[i] = shopping.NewShoppingTxnKVStoreService("tcp", ppt, coordAddr)
	}

	// coordinator
	shopping.NewShoppingTxnCoordinator(coordAddr,
		kvstoreAddrs, keyHashFunc, timeoutMs)

	shopping.InitService(appAddr, coordAddr, userCsv, itemCsv,
		kvstoreAddrs, keyHashFunc)
	block := make(chan bool)
	<-block
}

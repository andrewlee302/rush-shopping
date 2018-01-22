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
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"rush-shopping/shopping"
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
		appPort = "8080"
	}
	appAddr := fmt.Sprintf("%s:%s", appHost, appPort)

	kvstoreHost := os.Getenv("KVSTORE_HOST")
	kvstorePort := os.Getenv("KVSTORE_PORT")
	kvstoreAddr := fmt.Sprintf("%s:%s", kvstoreHost, kvstorePort)

	userCsv := os.Getenv("USER_CSV")
	itemCsv := os.Getenv("ITEM_CSV")

	ks := shopping.StartTransKVStore(kvstoreAddr)
	fmt.Println("here1")
	ks.Serve()
	shopping.InitService(appAddr, kvstoreAddr, userCsv, itemCsv)
	block := make(chan bool)
	<-block
}

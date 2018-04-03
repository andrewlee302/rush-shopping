package main

import (
	"distributed-system/twopc"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/pprof"
	"rush-shopping/shopping"
	"rush-shopping/util"
)

func main() {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	web := flag.Bool("s", false, "shopping web service")
	coord := flag.Bool("c", false, "shopping kvstore coordinator")
	parti := flag.Bool("p", false, "shopping kvstore participant")
	config := flag.String("f", "cfg.json", "config file")

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	cfg := util.ParseCfg(*config)

	keyHashFunc := twopc.DefaultKeyHashFunc

	blocked := false
	if *parti {
		for _, pptAddr := range cfg.KVStoreAddrs {
			if ip, _, err := net.SplitHostPort(pptAddr); err == nil {
				if _, err := net.LookupHost(ip); err == nil {
					blocked = true
					go shopping.NewShoppingTxnKVStoreService(cfg.Protocol, pptAddr, cfg.CoordinatorAddr)
				} else {
					fmt.Println(err)
				}
			}
		}
	}

	if *coord {
		if ip, _, err := net.SplitHostPort(cfg.CoordinatorAddr); err == nil {
			if _, err := net.LookupHost(ip); err == nil {
				blocked = true
				go shopping.NewShoppingTxnCoordinator(cfg.Protocol, cfg.CoordinatorAddr,
					cfg.KVStoreAddrs, keyHashFunc, cfg.TimeoutMS)
			} else {
				fmt.Println(err)
			}
		}
	}

	if *web {
		for _, appAddr := range cfg.APPAddrs {
			if ip, _, err := net.SplitHostPort(appAddr); err == nil {
				if _, err := net.LookupHost(ip); err == nil {
					blocked = true
					shopping.InitService(cfg.Protocol, appAddr, cfg.CoordinatorAddr, cfg.UserCSV, cfg.ItemCSV,
						cfg.KVStoreAddrs, keyHashFunc)
				}
			}

		}
	}
	if blocked {
		block := make(chan bool)
		<-block
	} else {
		flag.PrintDefaults()
	}
}

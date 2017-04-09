package main

import (
	"flag"

	"github.com/golang/glog"

	"github.com/crypto-bank/go-orderbook/server"
	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/exchange"
)

var (
	dbPath       = flag.String("db-path", "/tmp/orderbook", "Database path")
	currencyPair = flag.String("currency-pair", "", "Currency pair separated with \"_\"")
)

func main() {
	// Parse command line flags
	flag.CommandLine.Set("logtostderr", "true")
	flag.Parse()
	// Defer flushing logs at shutdown
	defer glog.Flush()

	// Create a new server
	srv, err := server.New(*dbPath)
	if err != nil {
		glog.Fatal(err)
	}

	// Defer closing a server
	defer func() {
		if err := srv.Close(); err != nil {
			glog.Fatal(err)
		}
	}()

	if *currencyPair == "" {
		glog.Fatal("--currency-pair flag is required")
	}

	// Parse currency pair
	pair, err := currency.ParsePair(*currencyPair, "_")
	if err != nil {
		glog.Fatal(err)
	}

	// Start syncing history
	err = server.StartSyncingHistory(srv, exchange.Poloniex, pair)
	if err != nil {
		glog.Fatal(err)
	}
}
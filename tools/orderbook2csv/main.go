package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"

	"github.com/golang/glog"

	"github.com/crypto-bank/go-orderbook/server"
	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/exchange"
)

var (
	dbPath       = flag.String("db-path", "/tmp/orderbook", "Database path")
	currencyPair = flag.String("currency-pair", "", "Currency pair to sync")
	outputFile   = flag.String("output", "", "CSV output file name")
	maxEntries   = flag.Int64("max-entries", 0, "Maximum entries (zero is all)")
)

func main() {
	// Parse command line flags
	flag.Parse()
	// Defer flushing logs at shutdown
	defer glog.Flush()

	if *outputFile == "" {
		glog.Fatal("--output flag is required")
	}

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

	// Parse currency pair
	pair, err := currency.ParsePair(*currencyPair, "_")
	if err != nil {
		glog.Fatal(err)
	}

	iter, err := srv.NewIterator(exchange.Poloniex, pair)
	if err != nil {
		glog.Fatal(err)
	}
	defer iter.Release()

	f, err := os.Create(*outputFile)
	if err != nil {
		glog.Fatal(err)
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	err = writer.Write([]string{
		"id",
		"timestamp",
		"rate",
		"volume",
		"total_price",
	})
	if err != nil {
		return
	}

	var sum int64
	for iter.Next() {
		trade, err := iter.Trade()
		if err != nil {
			glog.Fatal(err)

		}

		err = writer.Write([]string{
			fmt.Sprintf("%v", trade.ID),
			fmt.Sprintf("%v", trade.Time.Seconds),
			fmt.Sprintf("%.8f", trade.Order.Rate.Amount),
			fmt.Sprintf("%.8f", trade.Order.Volume.Amount),
			fmt.Sprintf("%.8f", trade.Order.TotalPrice().Amount),
		})
		if err != nil {
			glog.Fatal(err)
		}

		sum++

		// Stop on --max-entries
		if *maxEntries > 0 && sum > *maxEntries {
			break
		}
	}

	glog.Infof("Written %d entries", sum)

	if err := iter.Error(); err != nil {
		glog.Fatal(err)
	}
	if err := writer.Error(); err != nil {
		glog.Fatal(err)
	}
}

package main

import (
	"flag"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/crypto-bank/go-orderbook/server"
	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/exchange"
	"github.com/crypto-bank/proto/orderbook"
	"github.com/golang/glog"
)

var (
	port   = flag.Int("port", 8139, "Service listening port")
	dbPath = flag.String("db-path", "/tmp/orderbook", "Database path")
)

func main() {
	// Parse command line flags
	flag.Parse()
	// Defer flushing logs at shutdown
	defer glog.Flush()

	// Create a new server
	srv, err := server.New(&server.Options{
		DatabasePath: *dbPath,
	})
	if err != nil {
		glog.Fatal(err)
	}

	// Defer closing a server
	defer func() {
		if err := srv.Close(); err != nil {
			glog.Fatal(err)
		}
	}()

	// Start syncing history in background
	go func() {
		if err := server.StartSyncingHistory(srv, exchange.Poloniex, currency.NewPair("BTC", "XRP")); err != nil {
			glog.Fatal(err)
		}
	}()

	// Start TCP listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}

	// Create new gRPC server
	grpcServer := grpc.NewServer()

	// Register orderbook service server
	orderbook.RegisterOrderBookServer(grpcServer, srv)

	// Start serving gRPC service
	if err := grpcServer.Serve(listener); err != nil {
		glog.Fatal(err)
	}
}

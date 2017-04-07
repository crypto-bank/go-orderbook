package server

import (
	"context"
	"time"

	"github.com/crypto-bank/go-exchanges/poloniex"
	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/exchange"
	"github.com/crypto-bank/proto/order"
	"github.com/golang/glog"
)

// StartSyncingHistory - Syncs server trades history.
// Should be ran in a separate goroutine, should never exit.
// Syncs history every 60 seconds.
func StartSyncingHistory(server *Server, ex exchange.Exchange, pair *currency.Pair) error {
	// Get database handle
	db, err := server.openDB(ex, pair)
	if err != nil {
		return err
	}

	for {
		glog.Infof("Syncing history for pair %s", pair.Concat("/"))

		// Pair history request
		req := poloniex.HistoryRequest{Pair: pair}

		// If history was synced,
		// we are going to sync up to latest sync
		// otherwise we are still syncing to the bottom
		if db.historySynced > 0 {
			req.Start = time.Unix(db.historySynced, 0)
			req.End = time.Now()
		} else {
			req.End = time.Unix(db.lowestTimestamp, 0)
		}

		// Start fetching history since last in DB
		results := fetchHistory(req)

		// Write received trades in batches
		for batch := range results {
			if err := db.WriteTrades(batch); err != nil {
				glog.Errorf("Trades write error: %v", err)
			}
		}

		// If we hit this point it means we have synced
		// entire history to the point of `db.highestTimestamp`.
		// Now we have to sync from the point of it to `now`.
		db.setHistorySynced()

		// Repeat after 15 seconds
		<-time.After(time.Second * 15)
	}
}

func fetchHistory(req poloniex.HistoryRequest) (_ <-chan []*order.Trade) {
	// Create results channel
	results := make(chan []*order.Trade, 10)

	// Get all trades in goroutine
	go func() {
		// Timeout context for pulling the data
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		// Fetch history from poloniex
		if err := poloniex.History(ctx, req, results); err != nil {
			glog.Warningf("History fetch error: %v", err)
		}

		// Close results channel
		close(results)
	}()

	return results
}

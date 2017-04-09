package server

import (
	"sync"

	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/exchange"
	"github.com/crypto-bank/proto/orderbook"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Server - Orderbook service server.
type Server struct {
	// path - Database path.
	path string

	// dbs - Map of databases by exchange/pair ID.
	dbs map[string]*pairDatabase

	// dbsMutex - Mutex which governs databases map.
	dbsMutex *sync.RWMutex
}

// New - Constructs new orderbook service server.
// Opens on-disk LevelDB database and syncs in-memory databases.
func New(path string) (server *Server, err error) {
	return &Server{
		path:     path,
		dbs:      make(map[string]*pairDatabase),
		dbsMutex: new(sync.RWMutex),
	}, nil
}

// Compact - Compacts exchange and currency pair database.
func (server *Server) Compact(ex exchange.Exchange, pair *currency.Pair) (err error) {
	db, err := server.openDB(ex, pair)
	if err != nil {
		return
	}
	return db.db.CompactRange(util.Range{})
}

// NewIterator - Creates a new iterator for given currency pair on exchange.
func (server *Server) NewIterator(ex exchange.Exchange, pair *currency.Pair) (_ *Iterator, err error) {
	db, err := server.openDB(ex, pair)
	if err != nil {
		return
	}
	return db.NewIterator(), nil
}

// Read - Starts streaming real-time updates of an order book,
// and all trades happening in real-time.
// Batches are sent in `MaxBatchSize` when reading from history,
// or in received size in real-time when live streaming.
func (server *Server) Read(req *orderbook.ReadRequest, res orderbook.OrderBook_ReadServer) (err error) {
	// db, err := server.openDB(req.Exchange, req.Pair)
	// if err != nil {
	// 	return
	// }

	return
}

// Close - Closes server database.
func (server *Server) Close() (err error) {
	server.dbsMutex.Lock()
	defer server.dbsMutex.Unlock()
	for _, db := range server.dbs {
		db.Close()
	}
	return
}

// openDB - Opens or returns opened database.
func (server *Server) openDB(ex exchange.Exchange, pair *currency.Pair) (db *pairDatabase, err error) {
	server.dbsMutex.Lock()
	defer server.dbsMutex.Unlock()
	id := databaseID(ex, pair)
	db, ok := server.dbs[id]
	if ok {
		return db, nil
	}
	db, err = openPairDatabase(server.path, ex, pair)
	if err != nil {
		return
	}
	server.dbs[id] = db
	return
}

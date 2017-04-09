package server

import (
	"fmt"
	"path/filepath"

	"strconv"

	"github.com/crypto-bank/proto/currency"
	"github.com/crypto-bank/proto/exchange"
	"github.com/crypto-bank/proto/order"
	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// pairDatabase - Single currency pair database.
type pairDatabase struct {
	exchange exchange.Exchange
	pair     *currency.Pair
	db       *leveldb.DB

	// highestTimestamp - Timestamp of last trade written.
	// This is UNIX timestamp.
	highestTimestamp int64

	// lowestTimestamp - Timestamp of the youngest trade.
	lowestTimestamp int64

	// historySynced - More than zero when synced from `lowestTimestamp` to `highestTimestamp`.
	// It is a `highestTimestamp` saved on latest history sync.
	historySynced int64
}

var (
	highestTimestampKey = []byte("__highest_timestamp__")
	lowestTimestampKey  = []byte("__lowest_timestamp__")
	historySyncedKey    = []byte("__history_synced__")
)

// openPairDatabase - Opens database for single currency pair.
func openPairDatabase(path string, ex exchange.Exchange, pair *currency.Pair) (db *pairDatabase, err error) {
	db = &pairDatabase{pair: pair, exchange: ex}
	db.db, err = leveldb.OpenFile(filepath.Join(path, databaseID(ex, pair)), nil)
	if err != nil {
		return nil, err
	}
	db.highestTimestamp, err = readInt64(db.db, highestTimestampKey)
	if err != nil {
		return
	}
	db.lowestTimestamp, err = readInt64(db.db, lowestTimestampKey)
	if err != nil {
		return
	}
	db.historySynced, err = readInt64(db.db, historySyncedKey)
	if err != nil {
		return
	}
	return
}

// WriteTrades - Writes batch of trades.
// Saves last trade timestamp as last timestamp written if higher.
func (db *pairDatabase) WriteTrades(trades []*order.Trade) (err error) {
	// Database batch
	batch := new(leveldb.Batch)

	// It will be changed during writing
	highestTimestamp := db.highestTimestamp
	lowestTimestamp := db.lowestTimestamp

	for _, trade := range trades {
		// Marshal trade to protocol buffers
		body, err := trade.Marshal()
		if err != nil {
			return err
		}

		// Insert into batch
		batch.Put(int64ToBytes(trade.ID), body)

		// Set last timestamp if higher
		if trade.Time.Seconds > highestTimestamp {
			highestTimestamp = trade.Time.Seconds
		}
		// Set lowest timestamp if lower
		if trade.Time.Seconds < lowestTimestamp || lowestTimestamp == 0 {
			lowestTimestamp = trade.Time.Seconds
		}
	}

	// Put last timestamp to database
	batch.Put(lowestTimestampKey, int64ToBytes(lowestTimestamp))
	batch.Put(highestTimestampKey, int64ToBytes(highestTimestamp))

	glog.V(3).Infof("Writing batch of length %d", batch.Len())

	// Write batch to database
	err = db.db.Write(batch, nil)
	if err != nil {
		return
	}

	// Set last written timestamp in-memory
	// only if higher than existing
	if highestTimestamp > db.highestTimestamp {
		db.highestTimestamp = highestTimestamp
	}
	// Set lowest timestamp if lower than existing
	if lowestTimestamp < db.lowestTimestamp {
		db.lowestTimestamp = lowestTimestamp
	}

	return
}

// NewIterator - Creates a new iterator of whole database.
func (db *pairDatabase) NewIterator() *Iterator {
	return &Iterator{
		Iterator: db.db.NewIterator(&util.Range{Limit: []byte("_")}, nil),
	}
}

// Close - Closes leveldb database.
func (db *pairDatabase) Close() error {
	return db.db.Close()
}

func (db *pairDatabase) setHistorySynced() error {
	if err := db.db.Put(historySyncedKey, int64ToBytes(db.highestTimestamp), nil); err != nil {
		return err
	}
	db.historySynced = db.highestTimestamp
	return nil
}

// databaseID - Constructs string database ID from exchange ID and currency pair.
func databaseID(ex exchange.Exchange, pair *currency.Pair) string {
	return fmt.Sprintf("%s-%s", ex.String(), pair.Concat("_"))
}

func int64ToBytes(i int64) []byte {
	return []byte(fmt.Sprintf("%d", i))
}

func readInt64(db *leveldb.DB, key []byte) (int64, error) {
	body, err := db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(body), 10, 64)
}

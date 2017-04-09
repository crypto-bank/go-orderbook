package server

import (
	"github.com/crypto-bank/proto/order"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// Iterator - Trades iterator.
type Iterator struct {
	iterator.Iterator
}

// Trade - Unmarshals trade.
func (iter *Iterator) Trade() (res *order.Trade, err error) {
	res = new(order.Trade)
	err = res.Unmarshal(iter.Value())
	return
}

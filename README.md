# go-orderbook

[![GoDoc](https://godoc.org/github.com/crypto-bank/proto/orderbook?status.svg)](https://godoc.org/github.com/crypto-bank/proto/orderbook)

Orders and trades books for cryptocurrency [exchanges](https://github.com/crypto-bank/go-exchanges).

Order book is currently single node service and stores data in a local LevelDB database.

It is not designed to store private data but to be synchronized with public exchanges.

Currently is hard-coded for `Poloniex` support but design might change in the future.

## Service client

`go-orderbook` is a [gRPC service](https://github.com/crypto-bank/proto/blob/master/orderbook/orderbook.proto).

## Support

* [Poloniex](https://github.com/crypto-bank/go-exchanges/tree/master/poloniex)

## License

                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

package exchange

type Exchage interface {
	ListenMarketData(id int, pairs []string) error
	ListenMarketDataInPartition(pairs []string, maxPairsPerConn int)
}

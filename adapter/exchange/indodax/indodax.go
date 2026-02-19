package indodax

import (
	"sync"
	"time"

	"michaelyusak/go-market-ingestor.git/entity"

	"github.com/go-resty/resty/v2"
)

type indodax struct {
	baseUrl                 string
	publicWsToken           string
	wsScheme                string
	wsHost                  string
	wsPath                  string
	orderBookChanPrefix     string
	tradeActivityChanPrefix string

	client       *resty.Client
	tradeTimeout time.Duration

	pairs      map[string]entity.PairMeta
	orderbooks map[string]entity.Orderbook

	orderbookCh     chan entity.Orderbook
	tradeActivityCh chan entity.TradeActivity

	mu sync.Mutex
}

func NewClient(
	baseUrl,
	wsScheme,
	wsHost,
	wsPath,
	publicWsToken,
	orderBookChanPrefix,
	tradeActivityChanPrefix string,
	tradeTimeout time.Duration,
	orderbookCh chan entity.Orderbook,
	tradeActivityCh chan entity.TradeActivity,
) *indodax {
	return &indodax{
		baseUrl:                 baseUrl,
		wsScheme:                wsScheme,
		wsHost:                  wsHost,
		wsPath:                  wsPath,
		publicWsToken:           publicWsToken,
		orderBookChanPrefix:     orderBookChanPrefix,
		tradeActivityChanPrefix: tradeActivityChanPrefix,

		client:       resty.New(),
		tradeTimeout: tradeTimeout,

		pairs:      map[string]entity.PairMeta{},
		orderbooks: map[string]entity.Orderbook{},

		orderbookCh:     orderbookCh,
		tradeActivityCh: tradeActivityCh,
	}
}

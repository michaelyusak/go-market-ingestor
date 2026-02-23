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

	pairs map[string]entity.PairMeta

	tradeActivityCh []chan entity.TradeActivity

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
	tradeActivityCh []chan entity.TradeActivity,
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

		pairs: map[string]entity.PairMeta{},

		tradeActivityCh: tradeActivityCh,
	}
}

func (i *indodax) broadcastTradeActivity(ta entity.TradeActivity) {
	for _, ch := range i.tradeActivityCh {
		select {
		case ch <- ta:
			// sent successfully
		default:
			// channel not ready, skip or log
		}
	}
}

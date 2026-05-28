package binance

import (
	"michaelyusak/go-market-ingestor.git/entity"

	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/binance/binance-connector-go/common/v2/common"
)

type binance struct {
	client          *client.BinanceSpotClient
	tradeActivityCh []chan entity.TradeActivityV2
}

func NewAdapter(
	tradeActivityCh []chan entity.TradeActivityV2,
) *binance {
	conf := common.NewConfigurationWebsocketStreams(
		common.WithWsStreamsBasePath(common.SpotWebsocketStreamsProdUrl),
	)

	return &binance{
		client:          client.NewBinanceSpotClient(client.WithWebsocketStreams(conf)),
		tradeActivityCh: tradeActivityCh,
	}
}

func (i *binance) broadcastTradeActivity(ta entity.TradeActivityV2) {
	for _, ch := range i.tradeActivityCh {
		select {
		case ch <- ta:
			// sent successfully
		default:
			// channel not ready, skip or log
		}
	}
}

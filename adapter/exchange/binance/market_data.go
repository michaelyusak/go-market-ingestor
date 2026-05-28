package binance

import (
	"fmt"
	"strings"

	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/sirupsen/logrus"
)

func (b *binance) ListenMarketData(id int, pairs []string) error {
	streams := make([]string, 0, len(pairs))

	for _, s := range pairs {
		streams = append(
			streams,
			strings.ToLower(s),
		)
	}

	err := b.client.WebsocketStreams.Connect(streams)
	if err != nil {
		return fmt.Errorf("[adapter][exchange][binance][ListenMarketData] failed to connect to the streams: %w", err)
	}

	aggTradeHandler := func(atr models.AggTradeResponse) {
		logrus.WithField("symbol", *atr.S).Debug("[adapter][exchange][binance][ListenMarketData] new aggTrade message")

		err := b.processAggTrade(atr)
		if err != nil {
			logrus.
				WithError(err).
				WithField("symbol", *atr.S).
				Error("[adapter][exchange][binance][ListenMarketData][messageHandler]")
		}
	}

	for _, s := range pairs {
		handler, err := b.client.WebsocketStreams.WebSocketStreamsAPI.AggTrade().Symbol(strings.ToLower(s)).Execute()
		if err != nil {
			return fmt.Errorf("[adapter][exchange][binance][ListenMarketData] failed to execute streams: %w", err)
		}

		handler.On("message", aggTradeHandler)
	}

	return nil
}

func (b *binance) ListenMarketDataInPartition(pairs []string, maxPairsPerConn int) {
	id := 1
	for start := 0; start < len(pairs); start += maxPairsPerConn {
		end := min(start+maxPairsPerConn, len(pairs))

		shard := pairs[start:end]

		go b.ListenMarketData(id, shard)
		id++
	}
}

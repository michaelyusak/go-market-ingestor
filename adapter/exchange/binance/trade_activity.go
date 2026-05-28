package binance

import (
	"encoding/json"
	"fmt"
	"michaelyusak/go-market-ingestor.git/entity"

	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/shopspring/decimal"
)

func (b *binance) processAggTrade(data models.AggTradeResponse) error {
	var ta entity.TradeActivityV2

	if data.S == nil || data.P == nil || data.Q == nil || data.M == nil || data.E == nil {
		b, _ := json.Marshal(data)
		return fmt.Errorf("[adapter][exchange][binance][processAggTrade] invalid aggTrade payload [raw: %s]", string(b))
	}

	ta.Epoch = *data.E / 1000
	ta.Symbol = *data.S
	ta.Exchange = "binance"

	// side
	if *data.M {
		ta.Side = entity.TradeSideSell
	} else {
		ta.Side = entity.TradeSideBuy
	}

	// price
	price, err := decimal.NewFromString(*data.P)
	if err != nil {
		return fmt.Errorf("[adapter][exchange][binance][processAggTrade] invalid price: %w", err)
	}

	// base volume
	baseQty, err := decimal.NewFromString(*data.Q)
	if err != nil {
		return fmt.Errorf("[adapter][exchange][binance][processAggTrade] invalid qty: %w", err)
	}

	ta.Price = price
	ta.BaseVolume = baseQty
	ta.QuoteVolume = price.Mul(baseQty)

	// deterministic key
	ta.Key = fmt.Sprintf("%s-%d-%d", ta.Symbol, *data.A, *data.T)

	b.broadcastTradeActivity(ta)

	return nil
}

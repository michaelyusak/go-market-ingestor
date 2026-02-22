package indodax

import (
	"encoding/json"
	"fmt"
	"michaelyusak/go-market-ingestor.git/entity"

	"github.com/shopspring/decimal"
)

func (i *indodax) processTradeActivity(data json.RawMessage) error {
	var indodaxTradeActivityData [][]any

	err := json.Unmarshal(data, &indodaxTradeActivityData)
	if err != nil {
		return fmt.Errorf("[adapters][exchanges][indodax][processTradeActivity][json.Unmarshal] Unmarshal ticker")
	}

	seen := map[string]int{}

	for _, ta := range indodaxTradeActivityData {
		ts := int64(ta[1].(float64))
		seq := int64(ta[2].(float64))
		key := fmt.Sprintf("%d-%d", ts, seq)
		if _, ok := seen[key]; ok {
			continue
		}

		i.broadcastTradeActivity(i.convertTradeActivity(key, ta))

		seen[key] = 1
	}

	return nil
}

func (i *indodax) convertTradeActivity(key string, tradeActivityData []any) entity.TradeActivity {
	filledPriceDecimal := decimal.NewFromFloat(tradeActivityData[4].(float64))
	baseVolumeDecimal, _ := decimal.NewFromString(tradeActivityData[5].(string))
	tradedVolumeDecimal, _ := decimal.NewFromString(tradeActivityData[6].(string))

	var side entity.TradeSide

	switch tradeActivityData[3].(string) {
	case "buy":
		side = entity.TradeSideBuy
	case "sell":
		side = entity.TradeSideSell
	}

	return entity.TradeActivity{
		Epoch:        int64(tradeActivityData[1].(float64)),
		Pair:         tradeActivityData[0].(string),
		Side:         side,
		Exchange:     "indodax",
		FilledPrice:  filledPriceDecimal,
		BaseVolume:   baseVolumeDecimal,
		TradedVolume: tradedVolumeDecimal,
		Notional:     filledPriceDecimal.Mul(tradedVolumeDecimal),
		Key:          key,
	}
}

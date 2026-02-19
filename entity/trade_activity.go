package entity

import "github.com/shopspring/decimal"

type TradeSide string

const (
	TradeSideBuy  TradeSide = "buy"
	TradeSideSell TradeSide = "sell"
)

type TradeActivity struct {
	Epoch        int64           `json:"epoch"`
	Pair         string          `json:"pair"`
	Side         TradeSide       `json:"side"`
	Exchange     string          `json:"exchange"`
	FilledPrice  decimal.Decimal `json:"filled_price"`
	BaseVolume   decimal.Decimal `json:"base_volume"`
	TradedVolume decimal.Decimal `json:"traded_volume"`
	Notional     decimal.Decimal `json:"notional"`
	Key          string          `json:"key"`
}

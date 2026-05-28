package entity

import "github.com/shopspring/decimal"

type TradeSide string

const (
	TradeSideBuy  TradeSide = "buy"
	TradeSideSell TradeSide = "sell"
)

type TradeActivity struct {
	Epoch        int64           `json:"epoch"` // in seconds
	Pair         string          `json:"pair"`
	Side         TradeSide       `json:"side"`
	Symbol       string          `json:"symbol"`
	Exchange     string          `json:"exchange"`
	FilledPrice  decimal.Decimal `json:"filled_price"`
	BaseVolume   decimal.Decimal `json:"base_volume"`
	TradedVolume decimal.Decimal `json:"traded_volume"`
	Notional     decimal.Decimal `json:"notional"`
	Key          string          `json:"key"`
}

type TradeActivityV2 struct {
	Epoch    int64     `json:"epoch"` // in seconds
	Side     TradeSide `json:"side"`
	Symbol   string    `json:"symbol"`
	Exchange string    `json:"exchange"`

	Price       decimal.Decimal `json:"price"`        // price per base
	BaseVolume  decimal.Decimal `json:"base_volume"`  // always base asset
	QuoteVolume decimal.Decimal `json:"quote_volume"` // always quote asset

	Key string `json:"key"`
}

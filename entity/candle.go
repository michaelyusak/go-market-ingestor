package entity

import (
	"github.com/shopspring/decimal"
)

type Candle struct {
	Epoch    int64           `json:"epoch"`
	Exchange string          `json:"exchange"`
	Symbol   string          `json:"symbol"`
	Open     decimal.Decimal `json:"open"`
	High     decimal.Decimal `json:"high"`
	Low      decimal.Decimal `json:"low"`
	Close    decimal.Decimal `json:"close"`
	Volume   CandleVolume    `json:"volume"`

	//internal
	Dirty bool `json:"-"`
}

type CandleVolume struct {
	Total decimal.Decimal `json:"total"`
	Buy   decimal.Decimal `json:"buy"`
	Sell  decimal.Decimal `json:"sell"`
}

package entity

import "github.com/shopspring/decimal"

type Orderbook struct {
	Pair     string      `json:"pair"`
	Exchange string      `json:"exchange"`
	Bid      []BookLevel `json:"bid"`
	Ask      []BookLevel `json:"ask"`
}

type BookLevel struct {
	Price decimal.Decimal `json:"price"`
	Qty   decimal.Decimal `json:"qty"`
}

package entity

import "github.com/shopspring/decimal"

type PairMeta struct {
	ID              string
	BaseCurrency    string
	TradedCurrency  string
	PricePrecision  decimal.Decimal
	VolumePrecision decimal.Decimal
	MinBaseTrade    decimal.Decimal
	IsSuspended     bool
	IsMaintenance   bool
}

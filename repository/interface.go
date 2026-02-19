package repository

import (
	"context"
	"michaelyusak/go-market-ingestor.git/entity"
	"time"
)

type Trades interface {
	InsertMany(ctx context.Context, trades []entity.TradeActivity) error
}

type Candles1m interface {
	InsertOne(ctx context.Context, candle entity.Candle) error
	GetOne(ctx context.Context, timestamp time.Time, exchange, symbol string) (*entity.Candle, error)
	UpdateOne(ctx context.Context, candle entity.Candle) error
}

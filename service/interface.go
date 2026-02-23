package service

import (
	"context"
	"michaelyusak/go-market-ingestor.git/entity"
)

type Storage interface {
	IngestTradeActivity(ctx context.Context, fullSignalCh chan bool)
	ProcessTradesInBatch(ctx context.Context, fullSignalCh chan bool)
}

type Stream interface {
	CreateCandleStream(ctx context.Context, req entity.CreateStreamReq) (entity.CreateStreamRes, error)
	StreamCandles(ctx context.Context, ch chan []byte, channel, token string) error
	Stop(channel, token string) error
}

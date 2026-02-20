package service

import "context"

type Storage interface {
	IngestTradeActivity(ctx context.Context, fullSignalCh chan bool)
	ProcessTradesInBatch(ctx context.Context, fullSignalCh chan bool)
}

type Stream interface{}

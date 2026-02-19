package service

import (
	"context"
	"fmt"
	"michaelyusak/go-market-ingestor.git/entity"
	"michaelyusak/go-market-ingestor.git/repository"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type storage struct {
	tradesRepo      repository.Trades
	candles1mRepo   repository.Candles1m
	tradeActivityCh chan entity.TradeActivity
	candle1mBuffer  entity.Candle
	tradesBuffer    []entity.TradeActivity

	mu sync.Mutex
}

func NewStorage(
	tradesRepo repository.Trades,
	candles1mRepo repository.Candles1m,
	tradeActivityCh chan entity.TradeActivity,
) *storage {
	return &storage{
		tradesRepo:      tradesRepo,
		candles1mRepo:   candles1mRepo,
		tradeActivityCh: tradeActivityCh,
		candle1mBuffer:  entity.Candle{},
		tradesBuffer:    []entity.TradeActivity{},
	}
}

func (s *storage) Start() {
	ctx := context.Background()
	fullSignalCh := make(chan bool)

	go s.ProcessTradesInBatch(ctx, fullSignalCh)
	go s.IngestTradeActivity(ctx, fullSignalCh)
}

func (s *storage) IngestTradeActivity(ctx context.Context, fullSignalCh chan bool) {
	logrus.Info("[service][storage][IngestTradeActivity] ingesting trade activity")

	for {
		trade, ok := <-s.tradeActivityCh
		if !ok {
			logrus.
				WithField("trade", fmt.Sprintf("%+v", trade)).
				Warn("[service][storage][IngestTradeActivity] failed to read channel")
		}

		s.tradesBuffer = append(s.tradesBuffer, trade)

		if len(s.tradesBuffer) >= 100000 {
			fullSignalCh <- true
		}
	}
}

func (s *storage) ProcessTradesInBatch(ctx context.Context, fullSignalCh chan bool) {
	logrus.Info("[service][storage][IngestTradeActivity] ready to process trade activity")

	ticker1m := time.NewTicker(time.Minute)

	for {
		process := false

		select {
		case <-ticker1m.C:
			if len(s.tradesBuffer) > 0 {
				process = true
			}
		case <-fullSignalCh:
			if len(s.tradesBuffer) >= 100000 {
				process = true
			}
		}

		if !process {
			continue
		}

		s.mu.Lock()
		tradesCopy := append([]entity.TradeActivity(nil), s.tradesBuffer...)
		s.tradesBuffer = []entity.TradeActivity{}
		s.mu.Unlock()

		s.storeTrades(ctx, tradesCopy)
		s.update1mCandle(ctx, tradesCopy)
	}
}

func (s *storage) storeTrades(ctx context.Context, trades []entity.TradeActivity) {
	err := s.tradesRepo.InsertMany(ctx, trades)
	if err != nil {
		logrus.
			WithError(err).
			Error("[service][storage][storeTrades][tradesRepo.InsertMany]")
		return
	}

	logrus.
		WithField("length", len(trades)).
		Info("[service][storage][storeTrades] trades stored")
}

func (s *storage) update1mCandle(ctx context.Context, trades []entity.TradeActivity) {
	candleBuffers := map[time.Time]*entity.Candle{}

	for _, trade := range trades {
		tradeTime := time.Unix(trade.Epoch, 0)
		normalizedTime := time.Date(tradeTime.Year(), tradeTime.Month(), tradeTime.Day(), tradeTime.Hour(), tradeTime.Minute(), 0, 0, tradeTime.Location())

		buf, ok := candleBuffers[normalizedTime]
		if !ok {
			stored, err := s.candles1mRepo.GetOne(ctx, normalizedTime, trade.Exchange, trade.Pair)
			if err != nil {
				logrus.
					WithError(err).
					WithField("exchange", trade.Exchange).
					WithField("pair", trade.Pair).
					WithField("time", normalizedTime.String()).
					Error("[service][storage][update1mCandle][candles1mRepo.GetOne]")

				continue
			}

			if stored != nil {
				stored.Dirty = true
				buf = stored
			}
		}

		if buf == nil {
			buf = &entity.Candle{
				Epoch:    normalizedTime.Unix(),
				Pair:     trade.Pair,
				Exchange: trade.Exchange,
				Open:     trade.FilledPrice,
				High:     trade.FilledPrice,
				Low:      trade.FilledPrice,
				Close:    trade.FilledPrice,
				Volume:   trade.BaseVolume,
			}

			candleBuffers[normalizedTime] = buf
			continue
		}

		buf.Close = trade.FilledPrice
		buf.Volume = buf.Volume.Add(trade.BaseVolume)

		if trade.FilledPrice.GreaterThan(buf.High) {
			buf.High = trade.FilledPrice
		}

		if trade.FilledPrice.LessThan(buf.Low) {
			buf.Low = trade.FilledPrice
		}

		candleBuffers[normalizedTime] = buf
	}

	for _, candle := range candleBuffers {
		if candle.Dirty {
			err := s.candles1mRepo.UpdateOne(ctx, *candle)
			if err != nil {
				logrus.
					WithError(err).
					WithField("exchange", candle.Exchange).
					WithField("pair", candle.Pair).
					WithField("time", time.Unix(candle.Epoch, 0).String()).
					Error("[service][storage][update1mCandle][candles1mRepo.UpdateOne]")
				continue
			}

			logrus.
				WithField("exchange", candle.Exchange).
				WithField("pair", candle.Pair).
				WithField("time", time.Unix(candle.Epoch, 0).String()).
				Info("[service][storage][update1mCandle]  updated candle")

			continue
		}

		err := s.candles1mRepo.InsertOne(ctx, *candle)
		if err != nil {
			logrus.
				WithError(err).
				WithField("exchange", candle.Exchange).
				WithField("pair", candle.Pair).
				WithField("time", time.Unix(candle.Epoch, 0).String()).
				Error("[service][storage][update1mCandle][candles1mRepo.InsertOne]")
			continue
		}

		logrus.
			WithField("exchange", candle.Exchange).
			WithField("pair", candle.Pair).
			WithField("time", time.Unix(candle.Epoch, 0).String()).
			Info("[service][storage][update1mCandle] inserted candle")
	}
}

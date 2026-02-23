package service

import (
	"context"
	"encoding/json"
	"fmt"
	"michaelyusak/go-market-ingestor.git/common"
	"michaelyusak/go-market-ingestor.git/entity"
	"net/http"
	"sync"
	"time"

	"github.com/michaelyusak/go-helper/apperror"
	hEntity "github.com/michaelyusak/go-helper/entity"
	"github.com/michaelyusak/go-helper/helper"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

type streamHandler struct {
	candleSize hEntity.Duration

	token string

	cleanedAt time.Time
}

type candleState struct {
	candle entity.Candle
	size   time.Duration
}

type stream struct {
	tradeActivityCh chan entity.TradeActivity

	handlerMap map[string]streamHandler
	handlerTtl time.Duration
	tokenLen   int

	candles []candleState

	candlesSubscribers map[string]map[string]chan []byte

	mu sync.Mutex
}

func NewStream(
	tradeActivityCh chan entity.TradeActivity,
) *stream {
	return &stream{
		tradeActivityCh: tradeActivityCh,

		handlerMap: map[string]streamHandler{},
		handlerTtl: 24 * time.Hour,
		tokenLen:   20,

		candles: []candleState{
			{
				size:   time.Minute,
				candle: entity.Candle{},
			},
		},

		candlesSubscribers: map[string]map[string]chan []byte{},
	}
}

func (s *stream) Start() {
	go s.runStreamHandlerCleaner()

	tic := time.NewTicker(time.Second)

	go func() {
		for {
			select {
			case trade := <-s.tradeActivityCh:
				s.handleTrade(trade)
			case now := <-tic.C:
				s.handleTimeBoundary(now)
			}
		}
	}()
}

func (s *stream) handleTrade(trade entity.TradeActivity) {
	for i := range s.candles {
		closed := common.UpdateCandle(&s.candles[i].candle, s.candles[i].size, trade)
		if closed != nil {
			s.emitCandle(*closed, s.candles[i].size.String())
		}
	}
}

func (s *stream) handleTimeBoundary(now time.Time) {
	for i := range s.candles {
		state := &s.candles[i]

		bucket := now.Truncate(state.size).Unix()

		if state.candle.Epoch == 0 {
			continue
		}

		if bucket > state.candle.Epoch {
			closed := state.candle

			s.emitCandle(closed, state.size.String())

			s.rolloverCandle(&s.candles[i], bucket)
		}
	}
}

func (s *stream) rolloverCandle(state *candleState, newOpen int64) {
	prevClose := state.candle.Close

	state.candle = entity.Candle{
		Epoch:  newOpen,
		Open:   prevClose,
		High:   prevClose,
		Low:    prevClose,
		Close:  prevClose,
		Volume: decimal.Zero,
	}
}

func (s *stream) emitCandle(candle entity.Candle, size string) error {
	data, err := json.Marshal(candle)
	if err != nil {
		logrus.WithError(err).Error("[service][stream][emitCandle][json.Marshal]")
		return fmt.Errorf("[service][stream][emitCandle][json.Marshal] error: %w", err)
	}

	s.mu.Lock()
	chs := s.candlesSubscribers[size]
	s.mu.Unlock()

	for channel, ch := range chs {
		ch <- data
		logrus.
			WithField("channel", channel).
			Info("[service][stream][emitCandle] candle emited")
	}

	return nil
}

func (s *stream) runStreamHandlerCleaner() {
	tic := time.NewTicker(time.Hour)

	for {
		<-tic.C

		s.cleanStreamHandler()
	}
}

func (s *stream) cleanStreamHandler() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	newMap := map[string]streamHandler{}

	for ch, handler := range s.handlerMap {
		if now.Before(handler.cleanedAt) {
			newMap[ch] = handler
		}
	}

	s.handlerMap = newMap
}

func (s *stream) CreateCandleStream(ctx context.Context, req entity.CreateStreamReq) (entity.CreateStreamRes, error) {
	channelHash := helper.HashSHA512(fmt.Sprintf("%s%v", time.Duration(req.CandleSize).String(), time.Now().UnixMilli()))
	channel := fmt.Sprintf("ch:%s", channelHash)

	token := common.CreateRandomString(s.tokenLen)

	s.mu.Lock()
	s.handlerMap[channel] = streamHandler{
		candleSize: req.CandleSize,
		token:      token,
		cleanedAt:  time.Now().Add(s.handlerTtl),
	}
	s.mu.Unlock()

	return entity.CreateStreamRes{
		Channel: channel,
		Token:   token,
	}, nil
}

func (s *stream) StreamCandles(ctx context.Context, ch chan []byte, channel, token string) error {
	s.mu.Lock()
	handler, ok := s.handlerMap[channel]
	s.mu.Unlock()

	if !ok {
		logrus.Warn("[service][stream][StreamCandles] stream not found")

		return apperror.BadRequestError(apperror.AppErrorOpt{
			Code:    http.StatusNotFound,
			Message: "[service][stream][StreamCandles] stream not found",
		})
	}

	if handler.token != token {
		logrus.Warn("[service][stream][StreamCandles] invalid token")

		return apperror.UnauthorizedError(apperror.AppErrorOpt{
			Message: "[service][stream][StreamCandles] invalid token",
		})
	}

	sizeStr := time.Duration(handler.candleSize).String()

	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok = s.candlesSubscribers[sizeStr]
	if !ok {
		s.candlesSubscribers[sizeStr] = map[string]chan []byte{}
	}
	s.candlesSubscribers[sizeStr][channel] = ch

	logrus.
		WithField("channel", channel).
		Info("[service][stream][StreamCandles] channel subscribed")

	return nil
}

func (s *stream) Stop(channel, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	handler, ok := s.handlerMap[channel]

	if !ok {
		logrus.Warn("[service][stream][Stop] stream not found")

		return apperror.BadRequestError(apperror.AppErrorOpt{
			Code:    http.StatusNotFound,
			Message: "[service][stream][Stop] stream not found",
		})
	}

	if handler.token != token {
		logrus.Warn("[service][stream][Stop] invalid token")

		return apperror.UnauthorizedError(apperror.AppErrorOpt{
			Message: "[service][stream][Stop] invalid token",
		})
	}

	size := time.Duration(handler.candleSize).String()
	if chList, ok := s.candlesSubscribers[size]; ok {
		ch := chList[channel]
		delete(chList, channel)
		close(ch)
	}

	logrus.
		WithField("channel", channel).
		Info("[service][stream][Stop] stream unsubscribed")

	return nil
}

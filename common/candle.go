package common

import (
	"michaelyusak/go-market-ingestor.git/entity"
	"time"
)

func UpdateCandle(
	candle *entity.Candle,
	size time.Duration,
	trade entity.TradeActivity,
) (closed *entity.Candle) {

	sizeSec := int64(size.Seconds())
	bucket := trade.Epoch - (trade.Epoch % sizeSec)

	if candle.Epoch == 0 {
		InitCandle(candle, bucket, trade)
		return nil
	}

	if bucket < candle.Epoch {
		return nil
	}

	if bucket > candle.Epoch {
		closedCandle := *candle
		InitCandle(candle, bucket, trade)
		return &closedCandle
	}

	UpdateOHLC(candle, trade)
	return nil
}

func InitCandle(candle *entity.Candle, bucket int64, trade entity.TradeActivity) {
	candle.Epoch = bucket
	candle.Pair = trade.Pair
	candle.Exchange = trade.Exchange
	candle.Open = trade.FilledPrice
	candle.High = trade.FilledPrice
	candle.Low = trade.FilledPrice
	candle.Close = trade.FilledPrice
	candle.Volume = trade.BaseVolume
}

func UpdateOHLC(candle *entity.Candle, trade entity.TradeActivity) {
	candle.Volume = candle.Volume.Add(trade.BaseVolume)
	candle.Close = trade.FilledPrice

	if trade.FilledPrice.GreaterThan(candle.High) {
		candle.High = trade.FilledPrice
	}

	if trade.FilledPrice.LessThan(candle.Low) {
		candle.Low = trade.FilledPrice
	}
}

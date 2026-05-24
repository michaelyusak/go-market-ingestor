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
	candle.Symbol = trade.Symbol
	candle.Open = trade.FilledPrice
	candle.High = trade.FilledPrice
	candle.Low = trade.FilledPrice
	candle.Close = trade.FilledPrice
	candle.Volume = entity.CandleVolume{
		Total: trade.BaseVolume,
	}

	switch trade.Side {
	case entity.TradeSideBuy:
		candle.Volume.Buy = trade.BaseVolume
	case entity.TradeSideSell:
		candle.Volume.Sell = trade.BaseVolume
	}
}

func UpdateOHLC(candle *entity.Candle, trade entity.TradeActivity) {
	candle.Volume.Total = candle.Volume.Total.Add(trade.BaseVolume)

	switch trade.Side {
	case entity.TradeSideBuy:
		candle.Volume.Buy = candle.Volume.Buy.Add(trade.BaseVolume)
	case entity.TradeSideSell:
		candle.Volume.Sell = candle.Volume.Sell.Add(trade.BaseVolume)
	}

	candle.Close = trade.FilledPrice

	if trade.FilledPrice.GreaterThan(candle.High) {
		candle.High = trade.FilledPrice
	}

	if trade.FilledPrice.LessThan(candle.Low) {
		candle.Low = trade.FilledPrice
	}
}

package common

import (
	"michaelyusak/go-market-ingestor.git/entity"
	"time"
)

func UpdateCandle(
	candle *entity.Candle,
	size time.Duration,
	trade entity.TradeActivityV2,
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

func InitCandle(candle *entity.Candle, bucket int64, trade entity.TradeActivityV2) {
	candle.Epoch = bucket
	candle.Exchange = trade.Exchange
	candle.Symbol = trade.Symbol
	candle.Open = trade.Price
	candle.High = trade.Price
	candle.Low = trade.Price
	candle.Close = trade.Price
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

func UpdateOHLC(candle *entity.Candle, trade entity.TradeActivityV2) {
	candle.Volume.Total = candle.Volume.Total.Add(trade.BaseVolume)

	switch trade.Side {
	case entity.TradeSideBuy:
		candle.Volume.Buy = candle.Volume.Buy.Add(trade.BaseVolume)
	case entity.TradeSideSell:
		candle.Volume.Sell = candle.Volume.Sell.Add(trade.BaseVolume)
	}

	candle.Close = trade.Price

	if trade.Price.GreaterThan(candle.High) {
		candle.High = trade.Price
	}

	if trade.Price.LessThan(candle.Low) {
		candle.Low = trade.Price
	}
}

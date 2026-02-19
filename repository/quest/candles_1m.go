package quest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"michaelyusak/go-market-ingestor.git/entity"
	"time"
)

type candles1m struct {
	db *sql.DB
}

func NewCandles1m(db *sql.DB) *candles1m {
	return &candles1m{
		db: db,
	}
}

func (r *candles1m) InsertOne(ctx context.Context, candle entity.Candle) error {
	q := `
		INSERT INTO candles_1m
		(timestamp, exchange, symbol, open, high, low, close, volume)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `

	openFl, _ := candle.Open.Float64()
	highFl, _ := candle.High.Float64()
	lowFl, _ := candle.Low.Float64()
	closeFl, _ := candle.Close.Float64()
	volFl, _ := candle.Volume.Float64()

	_, err := r.db.ExecContext(ctx, q,
		time.Unix(candle.Epoch, 0),
		candle.Exchange,
		candle.Pair,
		openFl,
		highFl,
		lowFl,
		closeFl,
		volFl,
	)
	if err != nil {
		return fmt.Errorf("[repository][quest][candles1m][InsertOne][db.ExecContext] error: %w", err)
	}

	return nil
}

func (r *candles1m) GetOne(ctx context.Context, timestamp time.Time, exchange, symbol string) (*entity.Candle, error) {
	q := `
		SELECT timestamp, exchange, symbol, open, high, low, close, volume
		FROM candles_1m
		WHERE exchange = $1
			AND symbol = $2
			AND timestamp = $3
	`

	var candle entity.Candle
	var candleTs time.Time

	err := r.db.QueryRowContext(ctx, q, exchange, symbol, timestamp).Scan(
		&candleTs,
		&candle.Exchange,
		&candle.Pair,
		&candle.Open,
		&candle.High,
		&candle.Low,
		&candle.Close,
		&candle.Volume,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("[repository][quest][candles1m][GetOne][db.QueryRowContext] error: %w", err)
	}

	candle.Epoch = candleTs.Unix()

	return &candle, nil
}

func (r *candles1m) UpdateOne(ctx context.Context, candle entity.Candle) error {
	q := `
		UPDATE candles_1m
		SET open = $1, high = $2, low = $3, close = $4, volume = $5
		WHERE exchange = $6
			AND symbol = $7
			AND timestamp = $8
	`

	_, err := r.db.ExecContext(ctx, q,
		candle.Open,
		candle.High,
		candle.Low,
		candle.Close,
		candle.Volume,
		candle.Exchange,
		candle.Pair,
		time.Unix(candle.Epoch, 0),
	)
	if err != nil {
		return fmt.Errorf("[repository][quest][candles1m][UpdateOne][db.ExecContext] error: %w", err)
	}

	return nil
}

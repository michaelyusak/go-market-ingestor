package quest

import (
	"context"
	"database/sql"
	"fmt"
	"michaelyusak/go-market-ingestor.git/entity"
	"strings"
	"time"
)

type trades struct {
	db *sql.DB
}

func NewTrades(db *sql.DB) *trades {
	return &trades{
		db: db,
	}
}

func (r *trades) InsertMany(ctx context.Context, trades []entity.TradeActivity) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO trades (timestamp, exchange, symbol, price, quantity, side) VALUES ")

	vals := make([]any, 0, len(trades)*6)
	for i, trade := range trades {
		if i > 0 {
			sb.WriteString(",")
		}

		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d)", i*6+1, i*6+2, i*6+3, i*6+4, i*6+5, i*6+6)

		priceFl, _ := trade.FilledPrice.Float64()
		quantityFl, _ := trade.TradedVolume.Float64()

		vals = append(vals, time.Unix(trade.Epoch, 0), trade.Exchange, trade.Pair, priceFl, quantityFl, trade.Side)
	}

	_, err := r.db.ExecContext(ctx, sb.String(), vals...)
	if err != nil {
		return fmt.Errorf("[repository][quest][trades][InsertMany][db.ExecContext] error: %w", err)
	}

	return nil
}

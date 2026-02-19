package indodax

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"michaelyusak/go-market-ingestor.git/entity"
	indodaxEntity "michaelyusak/go-market-ingestor.git/entity/indodax"

	"github.com/shopspring/decimal"
)

func (i *indodax) processOrderbook(data json.RawMessage, pair string) error {
	var indodaxOrderbook indodaxEntity.IndodaxOrderBook

	err := json.Unmarshal(data, &indodaxOrderbook)
	if err != nil {
		return fmt.Errorf("[adapters][exchanges][indodax][processOrderbook][json.Unmarshal] Unmarshal orderbook: %w", err)
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	i.orderbooks[pair] = i.convertOrderbook(indodaxOrderbook, i.pairs[pair].TradedCurrency)

	return nil
}

func (i *indodax) convertOrderbook(indodaxOrderbook indodaxEntity.IndodaxOrderBook, coin string) entity.Orderbook {
	var bid, ask []entity.BookLevel

	priceField := "price"
	qtyField := coin + "_volume"

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for _, b := range indodaxOrderbook.Bid {
			priceDecimal, _ := decimal.NewFromString(b[priceField])
			qtyDecimal, _ := decimal.NewFromString(b[qtyField])

			bid = append(bid, entity.BookLevel{
				Price: priceDecimal,
				Qty:   qtyDecimal,
			})
		}

		sort.Slice(bid, func(i, j int) bool {
			return bid[i].Price.LessThan(bid[j].Price)
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for _, a := range indodaxOrderbook.Ask {
			priceDecimal, _ := decimal.NewFromString(a[priceField])
			qtyDecimal, _ := decimal.NewFromString(a[qtyField])

			ask = append(ask, entity.BookLevel{
				Price: priceDecimal,
				Qty:   qtyDecimal,
			})
		}

		sort.Slice(ask, func(i, j int) bool {
			return ask[i].Price.LessThan(ask[j].Price)
		})
	}()

	wg.Wait()

	return entity.Orderbook{
		Pair:     indodaxOrderbook.Pair,
		Exchange: "indodax",
		Bid:      bid,
		Ask:      ask,
	}
}

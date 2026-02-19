package indodax

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	indodaxEntity "michaelyusak/go-market-ingestor.git/entity/indodax"
)

type marketDataListenerEvent struct {
	Close bool
	Error error
	Info  string
}

func (i *indodax) ListenMarketData(id int, pairs []string) error {
	u := url.URL{Scheme: i.wsScheme, Host: i.wsHost, Path: i.wsPath}

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("[adapters][exchanges][indodax][ListenMarketData][websocket.DefaultDialer.Dial] Error: %w", err)
	}
	defer c.Close()

	quit := make(chan marketDataListenerEvent, 10)
	defer close(quit)

	clientId := time.Now().Unix() + int64(id)

	authMsg := indodaxEntity.IndodaxWsMessage{
		Params: indodaxEntity.IndodaxWsMessageParams{
			Token: i.publicWsToken,
		},
		Id: clientId,
	}

	authenticated := make(chan bool)

	go func() {
		messageType, data, err := c.ReadMessage()
		if err != nil {
			quit <- marketDataListenerEvent{
				Close: false,
				Error: err,
				Info:  "[adapters][exchanges][indodax][ListenMarketData][Auth][c.ReadMessage()]",
			}
			return
		}

		if messageType != websocket.TextMessage {
			quit <- marketDataListenerEvent{
				Close: false,
				Error: err,
				Info:  "[adapters][exchanges][indodax][ListenMarketData][Auth][messageTypeNotTextMessage)]",
			}
			return
		}

		var msg indodaxEntity.IndodaxWsResponse
		err = json.Unmarshal(data, &msg)
		if err != nil {
			quit <- marketDataListenerEvent{
				Close: false,
				Error: err,
				Info:  fmt.Sprintf("[adapters][exchanges][indodax][ListenMarketData][Auth][json.Unmarshal] [raw: %s]", string(data)),
			}
			return
		}

		if msg.Result.Client == "" {
			quit <- marketDataListenerEvent{
				Close: false,
				Error: err,
				Info:  fmt.Sprintf("[adapters][exchanges][indodax][ListenMarketData][Auth][json.Unmarshal] [raw: %s]", string(data)),
			}
			return
		}

		authenticated <- true
	}()

	c.WriteJSON(authMsg)

	<-authenticated
	close(authenticated)

	go func() {
	loop:
		for {
			messageType, data, err := c.ReadMessage()
			if err != nil {
				quit <- marketDataListenerEvent{
					Close: false,
					Error: err,
					Info:  "[adapters][exchanges][indodax][ListenMarketData][c.ReadMessage()]",
				}
				break loop
			}

			if messageType != websocket.TextMessage {
				continue
			}

			reader := bytes.NewReader(data)
			dec := json.NewDecoder(reader)

			for dec.More() {
				var msg indodaxEntity.IndodaxWsResponse
				err := dec.Decode(&msg)
				if err != nil {
					quit <- marketDataListenerEvent{
						Close: false,
						Error: err,
						Info:  fmt.Sprintf("[adapters][exchanges][indodax][ListenMarketData][dec.Decode] Unmarshal Response [raw: %s]", string(data)),
					}
					continue loop
				}

				if strings.HasPrefix(msg.Result.Channel, i.orderBookChanPrefix) {
					logrus.WithField("channel", msg.Result.Channel).Debug("[adapter][exchange][indodax][ListenMarketData] new orderbook message")
					err = i.processOrderbook(msg.Result.Data.Data, strings.Trim(msg.Result.Channel, i.orderBookChanPrefix))
					if err != nil {
						quit <- marketDataListenerEvent{
							Close: false,
							Error: err,
							Info:  "[adapters][exchanges][indodax][ListenMarketData][processOrderbook]",
						}
					}
					continue loop
				}

				if strings.HasPrefix(msg.Result.Channel, i.tradeActivityChanPrefix) {
					logrus.WithField("channel", msg.Result.Channel).Debug("[adapter][exchange][indodax][ListenMarketData] new trade activity message")
					err = i.processTradeActivity(msg.Result.Data.Data)
					if err != nil {
						quit <- marketDataListenerEvent{
							Close: false,
							Error: err,
							Info:  "[adapters][exchanges][indodax][ListenMarketData][processTradeActivity]",
						}
					}
					continue loop
				}
			}
		}

		quit <- marketDataListenerEvent{
			Close: false,
			Error: err,
			Info:  "[adapters][exchanges][indodax][ListenMarketData][LoopBroken]",
		}
	}()

	mapIdx := 1

	for _, pair := range pairs {
		orderbookChanName := i.orderBookChanPrefix + pair
		subscribeOrderbookMsg := indodaxEntity.IndodaxWsMessage{
			Method: 1,
			Params: indodaxEntity.IndodaxWsMessageParams{
				Channel: orderbookChanName,
			},
			Id: clientId,
		}
		c.WriteJSON(subscribeOrderbookMsg)
		logrus.WithFields(logrus.Fields{
			"id": id,
		}).Debugf("[adapter][exchanges][indodax][ListenMarketData] %v/%v subscribed to %s orderbook channel", mapIdx, len(pairs), pair)

		tradeActivityChanName := i.tradeActivityChanPrefix + pair
		subscribeTradeActivityMsg := indodaxEntity.IndodaxWsMessage{
			Method: 1,
			Params: indodaxEntity.IndodaxWsMessageParams{
				Channel: tradeActivityChanName,
			},
			Id: clientId,
		}
		c.WriteJSON(subscribeTradeActivityMsg)
		logrus.WithFields(logrus.Fields{
			"id": id,
		}).Debugf("[adapter][exchanges][indodax][ListenMarketData] %v/%v subscribed to %s trade activity channel", mapIdx, len(pairs), pair)

		mapIdx++
		time.Sleep(500 * time.Millisecond)
	}

	logrus.
		WithFields(logrus.Fields{
			"id": id,
		}).
		Info("[adapter][exchanges][indodax][ListenMarketData] market data websocket fully initiated")

	e := <-quit

	if e.Close {
		return fmt.Errorf("[%s] %s CLOSING Indodax market data listener. Error: %w", time.Now().String(), e.Info, e.Error)
	}

	logrus.
		WithFields(logrus.Fields{
			"id": id,
		}).
		WithError(e.Error).
		Errorf("RESTARTING Indodax market data listener. %s", e.Info)

	return i.ListenMarketData(id, pairs)
}

func (i *indodax) ListenMarketDataInPartition(pairs []string, maxPairsPerConn int) {
	id := 1
	for start := 0; start < len(pairs); start += maxPairsPerConn {
		end := min(start+maxPairsPerConn, len(pairs))

		shard := pairs[start:end]

		go i.ListenMarketData(id, shard)
		id++
	}
}

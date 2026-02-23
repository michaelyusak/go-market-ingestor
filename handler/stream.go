package handler

import (
	"context"
	"encoding/json"
	"errors"
	"michaelyusak/go-market-ingestor.git/common"
	"michaelyusak/go-market-ingestor.git/entity"
	"michaelyusak/go-market-ingestor.git/service"
	"sync"

	"github.com/gorilla/websocket"
	hHelper "github.com/michaelyusak/go-helper/helper"
	"github.com/sirupsen/logrus"

	"github.com/gin-gonic/gin"
)

type Stream struct {
	streamService service.Stream
	upgrader      websocket.Upgrader
}

func NewStream(
	streamService service.Stream,
	upgrader websocket.Upgrader,
) *Stream {
	return &Stream{
		streamService: streamService,
		upgrader:      upgrader,
	}
}

func (h *Stream) Create(ctx *gin.Context) {
	ctx.Header("Content-Type", "application/json")

	var req entity.CreateStreamReq

	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.Error(err)
		return
	}

	c := ctx.Request.Context()

	res, err := h.streamService.CreateCandleStream(c, req)
	if err != nil {
		ctx.Error(err)
		return
	}

	hHelper.ResponseOK(ctx, res)
}

func (h *Stream) Start(ctx *gin.Context) {
	conn, err := h.upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		ctx.Error(err)
		return
	}
	defer conn.Close()

	c, done := context.WithCancel(ctx.Request.Context())
	defer done()

	dataCh := make(chan []byte)

	var wg sync.WaitGroup
	var channel, token string
	var initialized bool

	wg.Add(1)
	// writer
	go func() {
		defer wg.Done()

	loop:
		for {
			select {
			case <-c.Done():
				logrus.Warn("[handler][Replay][StreamReplay][Write] closing loop")
				break loop
			case data := <-dataCh:
				err := conn.WriteMessage(websocket.TextMessage, data)
				if err != nil {
					if errors.Is(err, websocket.ErrCloseSent) {
						break
					}

					logrus.
						WithError(err).
						Warn("[handler][Replay][StreamReplay][Write][conn.WriteMessage]")
					continue
				}
			}
		}
	}()

	// listener
	go func() {

	loop:
		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err) {
					done()
					break loop
				}

				logrus.
					WithError(err).
					Warn("[handler][Replay][StreamReplay][Read][conn.ReadMessage]")

				continue
			}

			switch messageType {
			case websocket.TextMessage:
				var msg entity.WsMessage
				err = json.Unmarshal(message, &msg)
				if err != nil {
					logrus.
						WithError(err).
						WithField("raw", string(message)).
						Warn("[handler][Replay][StreamReplay][Read][json.Unmarshal(message, &msg)]")
					continue
				}

				if msg.Type == string(entity.WsMessageTypeAuth) {
					if initialized {
						logrus.
							WithField("channel", channel).
							Warn("[handler][Replay][StreamReplay][Read] getting auth message after initialized. continue.")
						continue loop
					}

					var authData entity.WsAuthData
					err = json.Unmarshal(msg.Data, &authData)
					if err != nil {
						logrus.
							WithError(err).
							WithField("raw", string(msg.Data)).
							Warn("[handler][Replay][StreamReplay][Read][json.Unmarshal(msg.Data, &authData)]")
						done()
						break loop
					}

					err = h.streamService.StreamCandles(c, dataCh, authData.Channel, authData.Token)
					if err != nil {
						logrus.
							WithError(err).
							WithField("channel", authData.Channel).
							Warn("[handler][Replay][StreamReplay][Read][streamService.StreamCandles]")
						done()
						break loop
					}

					initialized = true
					channel = authData.Channel
					token = authData.Token
				}

				if !initialized {
					logrus.
						Warn("[handler][Replay][StreamReplay][Read] getting message before initialized. breaking.")
					done()
					break loop
				}

				// Feature
			}
		}
	}()

	wg.Wait()

	logrus.
		WithField("channel", channel).
		Info("[handler][stream][Start] stopping stream")

	if initialized {
		h.streamService.Stop(channel, token)
	}

	err = common.CloseConn(conn)
	if err != nil {
		ctx.Error(err)
	}
}

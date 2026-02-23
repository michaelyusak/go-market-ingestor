package entity

import (
	"encoding/json"

	hEntity "github.com/michaelyusak/go-helper/entity"
)

type CreateStreamReq struct {
	CandleSize hEntity.Duration `json:"candle_size" form:"candle_size"`
}

type CreateStreamRes struct {
	Channel string `json:"channel"`
	Token   string `json:"token,omitempty"`
}

type WsMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type WsMessageType string

const (
	WsMessageTypeAuth WsMessageType = "auth"
)

type WsAuthData struct {
	Channel string `json:"channel"`
	Token   string `json:"token"`
}

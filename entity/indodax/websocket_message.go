package indodax

type IndodaxWsMessageParams struct {
	Channel string `json:"channel,omitempty"`
	Token   string `json:"token,omitempty"`
}

type IndodaxWsMessage struct {
	Method int                    `json:"method,omitempty"`
	Params IndodaxWsMessageParams `json:"params"`
	Id     int64                  `json:"id"`
}

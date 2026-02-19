package indodax

import "encoding/json"

type IndodaxWsResponseResultData struct {
	Data   json.RawMessage `json:"data"`
	Offset int64           `json:"offset"`
}

type IndodaxWsResponseResult struct {
	Channel string                      `json:"channel"`
	Data    IndodaxWsResponseResultData `json:"data"`
	Client  string                      `json:"client"`
	Version string                      `json:"version"`
	Expires bool                        `json:"expires"`
	Ttl     int64                       `json:"ttl"`
}

type IndodaxWsResponse struct {
	Result IndodaxWsResponseResult `json:"result"`
}

package indodax

type IndodaxOrderBook struct {
	Pair string                `json:"pair"`
	Bid  []IndodaxRawBookLevel `json:"bid"`
	Ask  []IndodaxRawBookLevel `json:"ask"`
}

type IndodaxRawBookLevel map[string]string

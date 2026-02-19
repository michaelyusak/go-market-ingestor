package config

import (
	"fmt"
	"os"

	hConfig "github.com/michaelyusak/go-helper/config"
	hEntity "github.com/michaelyusak/go-helper/entity"
)

type IndodaxConfig struct {
	BaseUrl                      string           `json:"base_url"`
	PublicWsToken                string           `json:"public_ws_token"`
	WsScheme                     string           `json:"ws_scheme"`
	WsHost                       string           `json:"ws_host"`
	WsPath                       string           `json:"ws_path"`
	OrderbookWsChannelPrefix     string           `json:"orderbook_ws_channel_prefix"`
	TradeActivityWsChannelPrefix string           `json:"trade_activity_ws_channel_prefix"`
	PairsToListen                map[string]bool  `json:"pairs_to_listen"`
	Timeout                      hEntity.Duration `json:"timeout"`
}

type ExchangeConfig struct {
	Indodax IndodaxConfig `json:"indodax"`
}

type LogConfig struct {
	Level string `json:"level"`
	Dir   string `json:"dir"`
}

type ServiceConfig struct {
	Port           string           `json:"port"`
	GracefulPeriod hEntity.Duration `json:"graceful_period"`
	Db             hEntity.DBConfig `json:"db"`
}

type CorsConfig struct {
	AllowedOrigins []string `json:"allowed_origins"`
}

type AppConfig struct {
	Service  ServiceConfig  `json:"service"`
	Log      LogConfig      `json:"log"`
	Exchange ExchangeConfig `json:"exchange"`
	Cors     CorsConfig     `json:"cors"`
}

func Init() (AppConfig, error) {
	configFilePath := os.Getenv("GO_MARKET_INGESTOR_CONFIG")

	var conf AppConfig

	conf, err := hConfig.InitFromJson[AppConfig](configFilePath)
	if err != nil {
		return conf, fmt.Errorf("[config][Init][hConfig.InitFromJson] Failed to init config from json: %w", err)
	}

	return conf, nil
}

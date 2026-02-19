package server

import (
	"michaelyusak/go-market-ingestor.git/adapter/exchange/indodax"
	"michaelyusak/go-market-ingestor.git/config"
	"michaelyusak/go-market-ingestor.git/entity"
	"michaelyusak/go-market-ingestor.git/repository/quest"
	"michaelyusak/go-market-ingestor.git/service"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	hAdaptor "github.com/michaelyusak/go-helper/adaptor"
	hHandler "github.com/michaelyusak/go-helper/handler"
	hMiddleware "github.com/michaelyusak/go-helper/middleware"
	"github.com/sirupsen/logrus"
)

type routerOpts struct {
	handler struct {
		common *hHandler.Common
	}
}

func newRouter(config *config.AppConfig) *gin.Engine {
	db, err := hAdaptor.ConnectDB(hAdaptor.PSQL, config.Service.Db)
	if err != nil {
		logrus.Panicf("Failed to connect to db: %v", err)
	}
	logrus.Info("Connected to postgres")

	orerbookCh := make(chan entity.Orderbook)
	tradeActivityCh := make(chan entity.TradeActivity)

	indodax := indodax.NewClient(
		config.Exchange.Indodax.BaseUrl,
		config.Exchange.Indodax.WsScheme,
		config.Exchange.Indodax.WsHost,
		config.Exchange.Indodax.WsPath,
		config.Exchange.Indodax.PublicWsToken,
		config.Exchange.Indodax.OrderbookWsChannelPrefix,
		config.Exchange.Indodax.TradeActivityWsChannelPrefix,
		time.Duration(config.Exchange.Indodax.Timeout),
		orerbookCh,
		tradeActivityCh,
	)

	tradesRepo := quest.NewTrades(db)
	candles1mRepo := quest.NewCandles1m(db)

	storageService := service.NewStorage(
		tradesRepo,
		candles1mRepo,
		tradeActivityCh,
	)

	commonHandler := hHandler.NewCommon(&APP_HEALTHY)

	storageService.Start()

	pairsToListen := []string{}
	for pair, listen := range config.Exchange.Indodax.PairsToListen {
		if listen {
			pairsToListen = append(pairsToListen, pair)
		}
	}
	indodax.ListenMarketDataInPartition(pairsToListen, 10)

	return createRouter(routerOpts{
		handler: struct {
			common *hHandler.Common
		}{
			common: commonHandler,
		},
	},
		config.Cors.AllowedOrigins,
	)
}

func createRouter(opts routerOpts, allowedOrigins []string) *gin.Engine {
	router := gin.New()

	corsConfig := cors.DefaultConfig()

	router.ContextWithFallback = true

	router.Use(
		hMiddleware.Logger(logrus.New()),
		hMiddleware.RequestIdHandlerMiddleware,
		hMiddleware.ErrorHandlerMiddleware,
		gin.Recovery(),
	)

	corsRouting(router, corsConfig, allowedOrigins)
	commonRouting(router, opts.handler.common)

	return router
}

func corsRouting(router *gin.Engine, corsConfig cors.Config, allowedOrigins []string) {
	corsConfig.AllowOrigins = allowedOrigins
	corsConfig.AllowMethods = []string{"POST", "GET", "PUT", "PATCH", "DELETE"}
	corsConfig.AllowHeaders = []string{"Origin", "Authorization", "Content-Type", "Accept", "User-Agent", "Cache-Control", "Device-Info", "X-Device-Id"}
	corsConfig.ExposeHeaders = []string{"Content-Length"}
	corsConfig.AllowCredentials = true
	router.Use(cors.New(corsConfig))
}

func commonRouting(router *gin.Engine, handler *hHandler.Common) {
	router.GET("/health", handler.Health)
	router.NoRoute(handler.NoRoute)
}

func staticRouting(router *gin.Engine, localStorageStaticPath, localStorageDirectory string) {
	router.Static(localStorageStaticPath, localStorageDirectory)
}

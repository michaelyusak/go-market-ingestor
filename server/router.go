package server

import (
	"michaelyusak/go-market-ingestor.git/adapter/exchange/indodax"
	"michaelyusak/go-market-ingestor.git/config"
	"michaelyusak/go-market-ingestor.git/entity"
	"michaelyusak/go-market-ingestor.git/handler"
	"michaelyusak/go-market-ingestor.git/repository/quest"
	"michaelyusak/go-market-ingestor.git/service"
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	hAdaptor "github.com/michaelyusak/go-helper/adaptor"
	hHandler "github.com/michaelyusak/go-helper/handler"
	hMiddleware "github.com/michaelyusak/go-helper/middleware"
	"github.com/sirupsen/logrus"
)

type routerOpts struct {
	handler struct {
		common *hHandler.Common
		stream *handler.Stream
	}
}

func newRouter(config *config.AppConfig) *gin.Engine {
	db, err := hAdaptor.ConnectDB(hAdaptor.PSQL, config.Service.Db)
	if err != nil {
		logrus.Panicf("Failed to connect to db: %v", err)
	}
	logrus.Info("Connected to postgres")

	tradeActivityStreamCh := make(chan entity.TradeActivity, 50)

	tradeActivityStorageCh := make(chan entity.TradeActivity, 50)

	tradeActivityCh := []chan entity.TradeActivity{tradeActivityStreamCh, tradeActivityStorageCh}

	indodax := indodax.NewClient(
		config.Exchange.Indodax.BaseUrl,
		config.Exchange.Indodax.WsScheme,
		config.Exchange.Indodax.WsHost,
		config.Exchange.Indodax.WsPath,
		config.Exchange.Indodax.PublicWsToken,
		config.Exchange.Indodax.OrderbookWsChannelPrefix,
		config.Exchange.Indodax.TradeActivityWsChannelPrefix,
		time.Duration(config.Exchange.Indodax.Timeout),
		tradeActivityCh,
	)

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	tradesRepo := quest.NewTrades(db)
	candles1mRepo := quest.NewCandles1m(db)

	storageService := service.NewStorage(
		tradesRepo,
		candles1mRepo,
		tradeActivityStorageCh,
	)
	streamService := service.NewStream(
		tradeActivityStreamCh,
	)

	commonHandler := hHandler.NewCommon(&APP_HEALTHY)
	streamHandler := handler.NewStream(
		streamService,
		upgrader,
	)

	storageService.Start()
	streamService.Start()

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
			stream *handler.Stream
		}{
			common: commonHandler,
			stream: streamHandler,
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
	streamRouting(router, opts.handler.stream)

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

func streamRouting(router *gin.Engine, handler *handler.Stream) {
	router.POST("/v1/stream/create", handler.Create)
	router.GET("/v1/stream/start", handler.Start)
}

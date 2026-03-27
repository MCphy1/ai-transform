package main

import (
	data2 "ai-transform-backend/data"
	"ai-transform-backend/pkg/config"
	"ai-transform-backend/pkg/db/mysql"
	"ai-transform-backend/pkg/log"
	"ai-transform-backend/pkg/mq/kafka"
	"ai-transform-backend/pkg/storage/cos"
	"ai-transform-backend/transform-web-api/controllers"
	"ai-transform-backend/transform-web-api/middleware"
	"ai-transform-backend/transform-web-api/routers"
	"flag"
	"fmt"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

var (
	configFile = flag.String("config", "dev.config.yaml", "config file path")
)

func main() {
	flag.Parse()
	config.InitConfig(*configFile)
	cfg := config.GetConfig()

	log.SetLevel(cfg.Log.Level)
	log.SetOutput(log.GetRotateWriter(cfg.Log.LogPath))
	log.SetPrintCaller(true)

	logger := log.NewLogger()
	logger.SetLevel(cfg.Log.Level)
	logger.SetOutput(log.GetRotateWriter(cfg.Log.LogPath))
	logger.SetPrintCaller(true)
	mysql.InitMysql(cfg)
	data := data2.NewData(mysql.GetDB())

	externalKafkaConf := &kafka.ProducerConfig{
		Config: kafka.Config{
			BrokerList:    cfg.ExternalKafka.Address,
			User:          cfg.ExternalKafka.User,
			Pwd:           cfg.ExternalKafka.Pwd,
			SASLMechanism: cfg.ExternalKafka.SaslMechanism,
			Version:       sarama.V3_7_0_0,
		},
		MaxRetry: cfg.ExternalKafka.MaxRetry,
	}
	kafka.InitKafkaProducer(kafka.ExternalProducer, externalKafkaConf)

	kafkaConf := &kafka.ProducerConfig{
		Config: kafka.Config{
			BrokerList:    cfg.Kafka.Address,
			User:          cfg.Kafka.User,
			Pwd:           cfg.Kafka.Pwd,
			SASLMechanism: cfg.Kafka.SaslMechanism,
			Version:       sarama.V3_7_0_0,
		},
		MaxRetry: cfg.Kafka.MaxRetry,
	}
	kafka.InitKafkaProducer(kafka.Producer, kafkaConf)

	cosStorageFactory := cos.NewCosStorageFactory(cfg.Cos.BucketUrl, cfg.Cos.SecretId, cfg.Cos.SecretKey, cfg.Cos.CDNDomain)
	cosUploadController := controllers.NewCosUpload(cfg, logger)
	transformController := controllers.NewTransform(cfg, logger, data, cosStorageFactory)

	gin.SetMode(cfg.Http.Mode)
	r := gin.Default()
	r.Use(middleware.Cors())
	r.GET("/health", func(*gin.Context) {})
	api := r.Group("/api")
	api.Use(middleware.Auth())
	routers.InitCosUploadRouters(api, cosUploadController)
	routers.InitTransformRouters(api, transformController)

	r.NoRoute(func(c *gin.Context) {
		// 如果是 API 请求，返回 404
		if len(c.Request.URL.Path) >= 4 && c.Request.URL.Path[:4] == "/api" {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		// 否则返回 index.html，让 Vue Router 处理前端路由
		http.ServeFile(c.Writer, c.Request, "www/index.html")
	})
	r.GET("/", func(c *gin.Context) {
		http.ServeFile(c.Writer, c.Request, "www/index.html")
	})
	// 静态资源路由
	r.Static("/assets", "www/assets")

	err := r.Run(fmt.Sprintf("%s:%d", cfg.Http.IP, cfg.Http.Port))
	if err != nil {
		log.Fatal(err)
	}
}

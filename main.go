package main

import (
	"flag"
	"net/http"
	"time"
	cfg "v-gnms/config"
	"v-gnms/pkg/core/database"
	. "v-gnms/pkg/core/gin_server"
	. "v-gnms/pkg/core/logger"
	"v-gnms/pkg/core/mqtt"
	. "v-gnms/pkg/core/rbac"
	"v-gnms/pkg/core/sns"
	"v-gnms/pkg/router"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
	"go.uber.org/ratelimit"
)

var (
	limit ratelimit.Limiter
)

func init() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Fatal().Msg("Error loading .env file")
	}
	cfg.InitializeConfig()

	// Use pretty logger only when in development
	// but not in production to save performance
	if cfg.GetConfigValue("GO_ENV") == "development" {
		log.Logger = GetLogger()
	}
	log.Info().Msg("Env config loaded and initialized.")
}

func leakBucket() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		_ = limit.Take()
	}
}

func main() {
	rps := flag.Int("rps", cfg.GetConfigValueAsInt("RATE_LIMITER_REQUEST_PER_SECOND"), "request per second")
	limit = ratelimit.New(*rps)

	// Initialize MQTT client
	mqtt.GetMqttClient()

	// Initialize MongoDB client
	database.GetMongoClient()

	// Initialize AWS SNS client
	sns.GetSnsClient()

	// Initialize Socket.IO server
	// socketio.GetSocketIOEngine(srv)

	// Initialize Gin web engine
	engine := GetGinEngine()

	// Apply the rate limiter
	engine.Use(leakBucket())

	InitializeCasbinRBAC()

	router.InitializeAPIRoutes()
	go startWateringScheduler()
	// Create server with timeout
	srv := &http.Server{
		Addr:    ":" + cfg.GetConfigValue("API_PORT"),
		Handler: engine,
		// Set timeout due CWE-400 - Potential Slowloris Attack
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Start server listening
	if cfg.GetConfigValue("USE_TLS") == "true" {
		certfilePath := cfg.GetConfigValue("TLS_CERTFILE_PATH")
		keyfilePath := cfg.GetConfigValue("TLS_KEYFILE_PATH")
		if err := srv.ListenAndServeTLS(certfilePath, keyfilePath); err != nil {
			log.Fatal().Any("Failed to start server: %v", err)
		}
	} else {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal().Any("Failed to start server: %v", err)
		}
	}
}

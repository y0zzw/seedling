package router

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	"v-gnms/pkg/api"
	"v-gnms/pkg/middleware"

	. "v-gnms/pkg/core/gin_server"
	. "v-gnms/pkg/core/socketio"
)

var routes *gin.RouterGroup

func InitializeAPIRoutes() {
	server := GetGinEngine()

	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true
	corsConfig.AllowCredentials = true
	corsConfig.AddAllowHeaders("Authorization")

	server.Use(cors.New(corsConfig), middleware.PreflightMiddleware())

	routes = server.Group("api/v1")

	routes.POST("auth/login", api.Login)
	routes.POST("auth/logout", api.Logout)
	// Watering schedule（不需要驗證，供 Unity 直接呼叫）
	routes.POST("devices/:device_id/watering-schedule", api.UploadWateringSchedule)
	routes.GET("devices/:device_id/watering-schedule", api.GetWateringSchedule)

	authenticatedRoutes := routes.Group("", middleware.JWTMiddleware)

	for _, route := range ROUTE_MAP {
		authenticatedRoutes.Handle(
			route.Method,
			route.Path,
			middleware.JWTMiddleware,
			middleware.CasbinMiddleware(route.Identifier),
			route.Handler,
		)
	}

	ExtractRBACActions()

	server.Use(middleware.SPAMiddleware("/", "./dist/"))

	socketio := GetSocketIOEngine()
	server.GET("/socket.io/*any", gin.WrapH(socketio.ServeHandler(nil)))
	server.POST("/socket.io/*any", gin.WrapH(socketio.ServeHandler(nil)))
}

func GetAPIRoutes() *gin.RouterGroup {
	return routes
}

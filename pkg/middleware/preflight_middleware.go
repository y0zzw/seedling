package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func PreflightMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// if cfg.GetConfigValue("CORS_ALLOW_ALL_ORIGINS") == "true" {
		c.Header("Access-Control-Allow-Origin", "*") //TODO: config.APIHost
		// }

		// else if os.Getenv("TLS_ENABLED") == "true" {
		// 	c.Header("Access-Control-Allow-Origin", fmt.Sprintf("https://%s", os.Getenv("TLS_DOMAIN_NAME"))) //TODO: config.APIHost
		// } else if config.SystemInfo.Frontend.Port == "80" {
		// 	c.Header("Access-Control-Allow-Origin", fmt.Sprintf("http://%s", config.SystemInfo.ExternalIP)) //TODO: config.APIHost

		// } else {
		// 	c.Header("Access-Control-Allow-Origin", fmt.Sprintf("http://%s:%s", config.SystemInfo.ExternalIP, config.SystemInfo.Frontend.Port)) //TODO: config.APIHost
		// }
		// if config.SystemInfo.DebugInfo.IsDebugEnabled {
		// 	c.Header("Access-Control-Allow-Origin", fmt.Sprintf("http://%s:%s", config.SystemInfo.DebugInfo.Frontend.Host, config.SystemInfo.DebugInfo.Frontend.Port))
		// }

		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Headers", "User-Agent, Token, session, Origin, Host,Accept-Encoding,Accept-Language,access-control-allow-origin, access-control-allow-headers, Connection,Sec-WebSocket-Key,Sec-WebSocket-Protocol,Sec-WebSocket-Version,Upgrade,Cache-Control,Pragma,Sec-Gpc")
		if c.Request.Method == "OPTIONS" {
			c.JSON(http.StatusNoContent, struct{}{})
			return
		}

		c.Next()
	}
}

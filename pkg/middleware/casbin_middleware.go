package middleware

import (
	"net/http"
	"v-gnms/pkg/core/rbac"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

func CasbinMiddleware(routeName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Retrieve the UID from the context passed by JWTMiddleware
		uid := c.GetString("uid")

		isAuthorized, err := rbac.CasbinEnforcer.Enforce(uid, routeName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Please contact system administrator."})
			c.Abort()
		}

		if !isAuthorized {
			c.JSON(http.StatusForbidden, gin.H{"error": "Unauthorized."})
			c.Abort()
		}

		log.Info().Msg(routeName)

		c.Next()
	}
}

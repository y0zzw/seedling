package middleware

import (
	"net/http"
	"strings"
	"v-gnms/pkg/core/jwt"

	"github.com/gin-gonic/gin"
)

func JWTMiddleware(c *gin.Context) {

	// Check presense of HTTP Authorization header
	// Returns [401] if header is not present
	authHeader := c.Request.Header.Get("Authorization")
	if authHeader == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Unauthorized"})
		c.Abort()
		return
	}

	// The header value will look like: "Bearer <token>"
	// Split the header value into two parts and make sure the token type is "Bearer"
	// Return [401] if it fails to split or not "Bearer"
	authParts := strings.SplitN(authHeader, " ", 2)
	if !(len(authParts) == 2 && authParts[0] == "Bearer" && len(strings.TrimSpace(authParts[1])) != 0 && authParts[1] != "null") {
		c.JSON(http.StatusUnauthorized, gin.H{"message": "Bad Authorization"})
		c.Abort()
		return
	}

	// Retrieve the actual JWT token
	authTokenValue := authParts[1]

	// Validate the JWT token
	// Returns [401] if validation fails
	tokenData, err := jwt.ParseJWTToken(authTokenValue)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"message": err.Error()})
		c.Abort()
		return
	}

	// Embed the UID and Email retrieved from the JWT token into the context for later use
	c.Set("uid", tokenData.UID)
	c.Set("email", tokenData.Email)

	c.Next()
}

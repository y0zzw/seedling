package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/contrib/static"
	"github.com/gin-gonic/gin"
)

// Gin-SPA middleware modified from https://github.com/mandrigin/gin-spa
// @author Quisette Chung (quisette.work@proton.me)
// @date 2024-4-1
func SPAMiddleware(urlPrefix, spaDirectory string) gin.HandlerFunc {
	directory := static.LocalFile(spaDirectory, true)
	fileserver := http.FileServer(directory)
	if urlPrefix != "" {
		fileserver = http.StripPrefix(urlPrefix, fileserver)
	}
	return func(c *gin.Context) {
		if directory.Exists(urlPrefix, c.Request.URL.Path) {
			fileserver.ServeHTTP(c.Writer, c.Request)
			c.Abort()
		} else {
			// prevents rendering files instead of geting socketio response
			if strings.HasPrefix(c.Request.URL.Path, "/socket.io") {
				c.Next()
				return
			}
			c.Request.URL.Path = "/"
			fileserver.ServeHTTP(c.Writer, c.Request)
			c.Abort()
		}
	}
}

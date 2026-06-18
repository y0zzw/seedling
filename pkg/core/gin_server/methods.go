package gin_server

import (
	"net/http"
	"reflect"
	"strings"

	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

var PATH_MODEL_MAPPING = map[string]reflect.Type{
	"roles": reflect.TypeOf((*Role)(nil)).Elem(),
}

func Get(c *gin.Context) {
	path := strings.Replace(c.Request.URL.Path, "/api/v1/", "", 1)
	log.Info().Str("", path).Msg("")

	targetType := PATH_MODEL_MAPPING[path]

	log.Info().Any("x", targetType).Msg("")

	c.JSON(http.StatusOK, nil)
}

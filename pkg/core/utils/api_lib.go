package utils

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DefaultApiListQueryParams struct {
	Start     int64
	Limit     int64
	SortBy    string
	SortOrder int64
	Fields    []string
}

func ExtractApiListQueryParams(c *gin.Context) DefaultApiListQueryParams {
	params := DefaultApiListQueryParams{}

	// Start
	start, err := strconv.Atoi(c.DefaultQuery("start", "0"))
	if err != nil {
		start = 0
	}
	params.Start = int64(start)

	// Count
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
	if err != nil {
		limit = 10
	}
	params.Limit = int64(limit)

	// Fields
	fields := c.DefaultQuery("fields", "")
	var fieldsArr []string
	if fields == "" {
		fieldsArr = []string{}
	} else {
		fieldsArr = strings.Split(fields, ",")
	}
	params.Fields = fieldsArr

	// Sort By
	params.SortBy = c.DefaultQuery("sort_by", "")

	// Sort Order
	sortOrder := c.DefaultQuery("sort_order", "")
	if sortOrder == "desc" {
		params.SortOrder = -1
	} else {
		params.SortOrder = 1
	}

	return params
}

func GenerateApiListOptions(queryParams DefaultApiListQueryParams) *options.FindOptions {
	opts := options.FindOptions{
		Skip:  &queryParams.Start,
		Limit: &queryParams.Limit,
	}

	if len(queryParams.Fields) > 0 {
		projection := bson.D{}
		for _, field := range queryParams.Fields {
			projection = append(projection, bson.E{Key: field, Value: 1})
		}
		opts.SetProjection(projection)
	}

	if queryParams.SortBy != "" {
		opts.SetSort(bson.D{{Key: queryParams.SortBy, Value: queryParams.SortOrder}})
	}

	return &opts
}

func ConstructListResponsePayload[T any](arr []T, itemCount int) map[string]interface{} {
	var responsePayload = make(map[string]interface{})

	responsePayload["count"] = itemCount

	items := []map[string]interface{}{}

	for _, item := range arr {
		res, _ := bson.MarshalExtJSON(item, false, false)

		var jsonMap map[string]interface{}
		json.Unmarshal(res, &jsonMap)

		items = append(items, jsonMap)
	}

	responsePayload["items"] = items

	return responsePayload
}

func ConstructGetSingleResponsePayload[T any](item T) map[string]interface{} {
	res, _ := bson.MarshalExtJSON(item, false, false)

	var jsonMap map[string]interface{}
	json.Unmarshal(res, &jsonMap)

	responsePayload := jsonMap

	return responsePayload
}

// This does not parse the JSON string into a map[string]interface{}.
// Only the original JSON string will be returned.
// Currently used by pkg/mqtt/message_handlers.go
func ConstructGetSingleResponsePayloadRaw[T any](item T) string {
	res, _ := bson.MarshalExtJSON(item, false, false)
	responsePayload := string(res)
	return responsePayload
}

func MergeStructs[T any](existing, update T) T {
	existingVal := reflect.ValueOf(&existing).Elem()
	updateVal := reflect.ValueOf(update)

	for i := 0; i < updateVal.NumField(); i++ {
		updateField := updateVal.Field(i)
		if !updateField.IsZero() {
			existingField := existingVal.Field(i)
			if existingField.CanSet() {
				existingField.Set(updateField)
			}
		}
	}

	return existing
}

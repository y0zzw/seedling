package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	. "v-gnms/pkg/core/dynamic_fields"
	. "v-gnms/pkg/core/mqtt"

	"github.com/gin-gonic/gin"
)

var MQTT_QUERY_DOWNLINK_PREFIX = "downlink/query"
var MQTT_COMMAND_DOWNLINK_PREFIX = "downlink/command"

func RequestDeviceQuery(c *gin.Context) {
	type RequestDeviceQueryStruct struct {
		QueryTarget     string                 `json:"query_target"`
		DownlinkPayload map[string]interface{} `json:"downlink_payload"`
	}
	var postData RequestDeviceQueryStruct

	if err := c.ShouldBindJSON(&postData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	deviceID := c.Param("id")

	payload, err := ExtractAndValidateQueryDownlinkPayload(postData.QueryTarget, postData.DownlinkPayload, deviceID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	payload["query_target"] = postData.QueryTarget

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	topic := fmt.Sprintf("%s/%s", MQTT_QUERY_DOWNLINK_PREFIX, deviceID)

	PublishMessageToTopic(topic, string(jsonPayload))

	c.JSON(http.StatusOK, gin.H{"message": "Query requested successfully"})
}

func RequestDeviceCommand(c *gin.Context) {
	type RequestDeviceCommandStruct struct {
		CommandName     string                 `json:"command_name"`
		DownlinkPayload map[string]interface{} `json:"downlink_payload"`
	}
	var postData RequestDeviceCommandStruct

	if err := c.ShouldBindJSON(&postData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	deviceID := c.Param("id")

	payload, err := ExtractAndValidateCommandDownlinkPayload(postData.CommandName, postData.DownlinkPayload, deviceID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	payload["command_name"] = postData.CommandName

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	topic := fmt.Sprintf("%s/%s", MQTT_COMMAND_DOWNLINK_PREFIX, deviceID)

	PublishMessageToTopic(topic, string(jsonPayload))

	c.JSON(http.StatusOK, gin.H{"message": "Command requested successfully"})
}

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/core/mqtt"
	. "v-gnms/pkg/core/utils"
	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

var MQTT_POLLING_DOWNLINK_PREFIX = "downlink/polling"

func ListDevicePollingResults(c *gin.Context) {
	queryParams := ExtractApiListQueryParams(c)

	pollingResults, itemCountInDB, err := FindMultiple[DevicePollingResult](DevicePollingResultsCollection, bson.D{}, GenerateApiListOptions(queryParams))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructListResponsePayload(pollingResults, itemCountInDB)
	c.JSON(http.StatusOK, responsePayload)
}

func ListDevicePollingResultsForDevice(c *gin.Context) {
	queryParams := ExtractApiListQueryParams(c)

	deviceId := c.Param("device_id")
	searchFilter := bson.D{bson.E{Key: "device_id", Value: deviceId}}

	pollingResults, itemCountInDB, err := FindMultiple[DevicePollingResult](DevicePollingResultsCollection, searchFilter, GenerateApiListOptions(queryParams))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructListResponsePayload(pollingResults, itemCountInDB)
	c.JSON(http.StatusOK, responsePayload)
}

func RequestDevicePolling(c *gin.Context) {

	deviceID := c.Param("id")

	// Reset the latest polling status cache in the device record
	// Returns [404] if the device does not exist
	resetStatus := 0
	device, err := FindSingleByID[Device](DevicesCollection, deviceID, nil)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Device not found."})
		return
	}
	device.LatestPollingStatus = &resetStatus
	device, _ = Update[Device](DevicesCollection, deviceID, device)

	var pollingResult DevicePollingResult

	pollingResult.ID = GenerateUUIDv7()
	pollingResult.DeviceID = deviceID

	status := 0
	pollingResult.Status = &status

	pollingResult.RequestedAt = GetCurrentTimestampSeconds()
	pollingResult.RespondedAt = -1

	pollingResult, err = Create[DevicePollingResult](DevicePollingResultsCollection, pollingResult)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	topic := fmt.Sprintf("%s/%s", MQTT_POLLING_DOWNLINK_PREFIX, pollingResult.DeviceID)
	payload := map[string]interface{}{
		"record_id":    pollingResult.ID,
		"requested_at": pollingResult.RequestedAt,
	}
	message, _ := json.Marshal(payload)

	PublishMessageToTopic(topic, string(message))

	c.JSON(http.StatusOK, gin.H{"message": "Polling requested successfully"})
}

func DeleteDevicePollingResult(c *gin.Context) {
	id := c.Param("id")

	pollingResult, err := Delete[DevicePollingResult](DevicePollingResultsCollection, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(pollingResult)
	c.JSON(http.StatusOK, responsePayload)
}

package api

import (
	"net/http"
	"strconv"
	"time"
	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/core/utils"
	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

func ListDeviceReports(c *gin.Context) {
	queryParams := ExtractApiListQueryParams(c)

	deviceReports, itemCountInDB, err := FindMultiple[DeviceReport](DeviceReportsCollection, bson.D{}, GenerateApiListOptions(queryParams))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serializedItems := SerializeDeviceReportsInBatch(&deviceReports)

	responsePayload := ConstructListResponsePayload(serializedItems, itemCountInDB)
	c.JSON(http.StatusOK, responsePayload)
}

func ListDeviceReportsForDevice(c *gin.Context) {
	queryParams := ExtractApiListQueryParams(c)

	deviceId := c.Param("device_id")

	var searchFilter bson.M

	var reportType int
	reportType, err := strconv.Atoi(c.Query("report_type"))
	timeRange := c.Query("time_range")

	var hoursToSubtract time.Duration
	if timeRange == "24h" {
		hoursToSubtract = -24 * time.Hour
	} else if timeRange == "7d" {
		hoursToSubtract = -24 * time.Hour * 7
	} else if timeRange == "30d" {
		hoursToSubtract = -24 * time.Hour * 30
	} else if timeRange == "90d" {
		hoursToSubtract = -24 * time.Hour * 90
	} else if timeRange == "180d" {
		hoursToSubtract = -24 * time.Hour * 180
	} else if timeRange == "ytd" {
		hoursToSubtract = -24 * time.Hour * 365
	} else {
		hoursToSubtract = -3 * time.Hour
	}
	startTime := time.Now().Add(hoursToSubtract).Unix()

	if err != nil {
		searchFilter = bson.M{"device_id": deviceId}
	} else {
		searchFilter = bson.M{
			"device_id":   deviceId,
			"type":        reportType,
			"reported_at": bson.M{"$gte": startTime},
		}
	}

	deviceReports, itemCountInDB, err := FindMultiple[DeviceReport](DeviceReportsCollection, searchFilter, GenerateApiListOptions(queryParams))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	maxItemCount := 500
	itemCount := len(deviceReports)

	interval := itemCount / maxItemCount
	if itemCount%maxItemCount != 0 {
		interval++ // Round up if there's a remainder
	}

	var reducedResults []DeviceReport
	for i := 0; i < itemCount; i++ {
		if i%interval == 0 && len(reducedResults) < maxItemCount {
			reducedResults = append(reducedResults, deviceReports[i])
		}
	}

	responsePayload := ConstructListResponsePayload(reducedResults, itemCountInDB)
	c.JSON(http.StatusOK, responsePayload)
}

func GetSingleDeviceReport(c *gin.Context) {
	id := c.Param("id")

	deviceReport, err := FindSingleByID[DeviceReport](DeviceReportsCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serializedItem := SerializeDeviceReport(&deviceReport)

	responsePayload := ConstructGetSingleResponsePayload(serializedItem)
	c.JSON(http.StatusOK, responsePayload)
}

func DeleteDeviceReport(c *gin.Context) {
	id := c.Param("id")

	deviceReport, err := Delete[DeviceReport](DeviceReportsCollection, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(deviceReport)
	c.JSON(http.StatusOK, responsePayload)
}


package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/core/mqtt"
	"v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
)

func UploadWateringSchedule(c *gin.Context) {
	deviceId := c.Param("device_id")

	var payload models.WateringSchedule
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	payload.ID = GenerateUUIDv7()
	payload.DeviceID = deviceId

	created, err := Create[models.WateringSchedule](WateringSchedulesCollection, payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 發送澆水排程給 AMB82
	scheduleMsg := map[string]interface{}{
		"command_name": "set_watering_schedule",
		"start_date":   created.StartDate,
		"schedule":     created.Schedule,
		"day_count":    created.DayCount,
	}
	jsonMsg, err := json.Marshal(scheduleMsg)
	if err == nil {
		log.Printf("[DEBUG] Publishing to topic:downlink/command/%s",deviceId)
		topic := fmt.Sprintf("downlink/command/%s", deviceId)
		PublishMessageToTopic(topic, string(jsonMsg))
		log.Printf("[DEBUG] JSON payload:%s",string(jsonMsg))
	}

	c.JSON(http.StatusOK, created)
}

func GetWateringSchedule(c *gin.Context) {
	deviceId := c.Param("device_id")

	schedule, err := FindSingleByField[models.WateringSchedule](
		WateringSchedulesCollection, "device_id", deviceId, nil)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no schedule found"})
		return
	}
	c.JSON(http.StatusOK, schedule)
}

package api

import (
	"net/http"

	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/core/dynamic_fields"
	. "v-gnms/pkg/core/filesystem"
	. "v-gnms/pkg/core/utils"
	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
)

func ListDevices(c *gin.Context) {
	queryParams := ExtractApiListQueryParams(c)

	devices, itemCountInDB, err := FindMultiple[Device](DevicesCollection, bson.D{}, GenerateApiListOptions(queryParams))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serializedItems := SerializeDevicesInBatch(&devices)

	responsePayload := ConstructListResponsePayload(serializedItems, itemCountInDB)
	c.JSON(http.StatusOK, responsePayload)
}

func GetSingleDevice(c *gin.Context) {
	id := c.Param("id")

	device, err := FindSingleByID[Device](DevicesCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serializedItem := SerializeDevice(&device)

	responsePayload := ConstructGetSingleResponsePayload(serializedItem)
	c.JSON(http.StatusOK, responsePayload)
}

func CreateDevice(c *gin.Context) {
	type CreateDeviceStruct struct {
		DeviceDefinitionID string `json:"device_definition_id" binding:"required"`
		IsActive           bool   `json:"is_active"`
	}
	var postData CreateDeviceStruct

	// Returns [400] if data is malformed
	if err := c.ShouldBindJSON(&postData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	deviceDefinition, err := FindSingleByID[DeviceDefinition](DeviceDefinitionsCollection, postData.DeviceDefinitionID, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Device definition not found."})
		return
	}

	// Try to parse the YAML file to verify its type
	// Returns [400] upon parsing failure
	yamlContentInJson, err := ReadYAMLFileAsJSON(deviceDefinition.DefinitionFilePath)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var item Device

	// Apply the ID and saved file path
	item.ID = GenerateUUIDv7()
	item.DeviceDefinitionID = postData.DeviceDefinitionID
	item.DataFields = ConstructDynamicDeviceFieldsFromYAMLData(yamlContentInJson)
	item.IsActive = &(postData.IsActive)

	// Insert the device into DB
	device, err := Create[Device](DevicesCollection, item)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(device)
	c.JSON(http.StatusOK, responsePayload)
}

func UpdateDevice(c *gin.Context) {
	type UpdateDeviceStruct struct {
		DataFields map[string]interface{} `json:"data_fields" binding:"required"`
		IsActive   *bool                  `json:"is_active" binding:"required"`
	}
	var patchData UpdateDeviceStruct

	// Returns [400] if data is malformed
	if err := c.ShouldBindJSON(&patchData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id := c.Param("id")

	// Find the target item
	// Returns [404] if not found
	existingItem, err := FindSingleByID[Device](DevicesCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Device not found."})
	}

	// Try to parse the YAML file to verify its type
	// Returns [400] upon parsing failure
	yamlContentInJson, err := GetDeviceDefinitionYAMLAsJSONFromDeviceDefinitionID(existingItem.DeviceDefinitionID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Extract and validate dynamic data
	// Returns [400] if validation fails
	validatedDataFields, err := ExtractAndValidateDynamicData(yamlContentInJson, existingItem.DataFields, patchData.DataFields)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Info().Any("", validatedDataFields).Msg("")

	existingItem.IsActive = patchData.IsActive
	existingItem.DataFields = validatedDataFields

	// Update to DB
	updatedItem, err := Update[Device](DevicesCollection, id, existingItem)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(updatedItem)
	c.JSON(http.StatusOK, responsePayload)
}

func DeleteDevice(c *gin.Context) {
	id := c.Param("id")

	device, err := Delete[Device](DevicesCollection, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(device)
	c.JSON(http.StatusOK, responsePayload)
}

/**
 * This is currently disabled (replaced HTTP by MQTT)
 */
// func GetDeviceUuidByDeviceID(c *gin.Context) {
// 	deviceID := c.Param("device_id")

// 	device, err := FindSingleByField[Device](DevicesCollection, "data_fields.device_id", deviceID, nil)
// 	if err != nil {
// 		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
// 		return
// 	}

// 	var responsePayload = make(map[string]interface{})
// 	responsePayload["device_uuid"] = device.ID

// 	c.JSON(http.StatusOK, responsePayload)
// }

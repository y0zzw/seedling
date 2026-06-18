package api

import (
	"net/http"

	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/core/filesystem"
	. "v-gnms/pkg/core/utils"
	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/bson"
)

var DEVICE_DEFINITION_STORAGE_PREFIX = "media/device_definitions"

func ListDeviceDefinitions(c *gin.Context) {
	queryParams := ExtractApiListQueryParams(c)

	deviceDefinitions, itemCountInDB, err := FindMultiple[DeviceDefinition](DeviceDefinitionsCollection, bson.D{}, GenerateApiListOptions(queryParams))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serializedItems := SerializeDeviceDefinitionsInBatch(&deviceDefinitions)

	responsePayload := ConstructListResponsePayload(serializedItems, itemCountInDB)
	c.JSON(http.StatusOK, responsePayload)
}

func GetSingleDeviceDefinition(c *gin.Context) {
	id := c.Param("id")

	deviceDefinition, err := FindSingleByID[DeviceDefinition](DeviceDefinitionsCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serializedItem := SerializeDeviceDefinition(&deviceDefinition)

	responsePayload := ConstructGetSingleResponsePayload(serializedItem)
	c.JSON(http.StatusOK, responsePayload)
}

func FetchDeviceDefinitionFileByID(c *gin.Context) {
	id := c.Param("id")

	result, err := GetDeviceDefinitionYAMLAsJSONFromDeviceDefinitionID(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func CreateDeviceDefinition(c *gin.Context) {

	itemId := GenerateUUIDv7()

	// Returns [400] upon saving failure
	path, err := SaveGinFile(c, DEVICE_DEFINITION_STORAGE_PREFIX, itemId, "yaml")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Try to parse the YAML file to verify its type
	// Returns [400] upon parsing failure
	yamlContentInJson, err := ReadYAMLFileAsJSON(path)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var item DeviceDefinition

	// Apply the ID and saved file path
	item.ID = itemId
	item.Name = gjson.Get(yamlContentInJson, "display_name").String()
	item.PreviousDefinitionFilePath = "null"
	item.DefinitionFilePath = path
	// *item.IsActive = true

	// Insert the deviceDefinition into DB
	deviceDefinition, err := Create[DeviceDefinition](DeviceDefinitionsCollection, item)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(deviceDefinition)
	c.JSON(http.StatusOK, responsePayload)
}

func UpdateDeviceDefinition(c *gin.Context) {
	id := c.Param("id")

	// Returns [400] upon saving failure
	path, err := SaveGinFile(c, DEVICE_DEFINITION_STORAGE_PREFIX, id, "yaml")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Try to parse the YAML file to verify its type
	// Returns [400] upon parsing failure
	yamlContentInJson, err := ReadYAMLFileAsJSON(path)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var newItem DeviceDefinition

	// Find the target item
	// Returns [404] if not found
	existingItem, err := FindSingleByID[DeviceDefinition](DeviceDefinitionsCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Device definition not found."})
	}

	outdatedFilePathToBeRemoved := existingItem.PreviousDefinitionFilePath
	if outdatedFilePathToBeRemoved != "null" {
		err = DeleteFile(outdatedFilePathToBeRemoved)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Please contact system administrator."})
			return
		}
	}

	// Prevent the ID from being modified
	newItem.ID = existingItem.ID
	newItem.PreviousDefinitionFilePath = existingItem.DefinitionFilePath
	newItem.DefinitionFilePath = path
	newItem.Name = gjson.Get(yamlContentInJson, "display_name").String()

	// Merge the new values into the existing item
	newItem = MergeStructs(existingItem, newItem)

	// Update to DB
	updatedItem, err := Update[DeviceDefinition](DeviceDefinitionsCollection, id, newItem)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(updatedItem)
	c.JSON(http.StatusOK, responsePayload)
}

func DeleteDeviceDefinition(c *gin.Context) {
	id := c.Param("id")

	item, err := FindSingleByID[DeviceDefinition](DeviceDefinitionsCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Item not found."})
		return
	}

	path := item.DefinitionFilePath
	err = DeleteFile(path)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Please contact system administrator."})
		return
	}

	deviceDefinition, err := Delete[DeviceDefinition](DeviceDefinitionsCollection, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(deviceDefinition)
	c.JSON(http.StatusOK, responsePayload)
}

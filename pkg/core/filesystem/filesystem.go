package filesystem

import (
	"fmt"
	"net/http"
	"os"
	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"sigs.k8s.io/yaml"
)

func SaveGinFile(c *gin.Context, pathPrefix string, itemId string, fileExtension string) (string, error) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File not found in request."})
		return "", err
	}

	timestampNow := GetCurrentTimestampSeconds()

	path := fmt.Sprintf("%s/%s__%d.%s", pathPrefix, itemId, timestampNow, fileExtension)

	c.SaveUploadedFile(file, path)

	return path, nil
}

func DeleteFile(path string) error {
	err := os.Remove(path)
	return err
}

func ReadYAMLFileAsJSON(path string) (string, error) {
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	json, err := yaml.YAMLToJSON(yamlFile)
	if err != nil {
		return "", err
	}

	return string(json), nil
}

func GetDeviceDefinitionYAMLAsJSONFromDeviceDefinitionID(deviceDefinitionId string) (string, error) {
	// Get the device definition
	deviceDefinition, err := FindSingleByID[DeviceDefinition](DeviceDefinitionsCollection, deviceDefinitionId, nil)
	if err != nil {
		return "", err
	}

	// Try to parse the YAML file to verify its type
	// Returns [400] upon parsing failure
	yamlContentInJson, err := ReadYAMLFileAsJSON(deviceDefinition.DefinitionFilePath)
	if err != nil {
		return "", err
	}

	return yamlContentInJson, nil
}

func GetDeviceDefinitionYAMLAsJSONFromDeviceID(deviceId string) (string, error) {
	device, err := FindSingleByID[Device](DevicesCollection, deviceId, nil)
	if err != nil {
		return "", err
	}

	return GetDeviceDefinitionYAMLAsJSONFromDeviceDefinitionID(device.DeviceDefinitionID)
}

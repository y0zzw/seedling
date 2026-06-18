package models

import (
	"encoding/json"
	"os"

	"sigs.k8s.io/yaml"
)

type DeviceDefinition struct {
	ID                         string `json:"id"`
	Name                       string `json:"name" bson:"name,omitempty"`
	DefinitionFilePath         string `json:"definition_file_path" bson:"definition_file_path,omitempty"`
	PreviousDefinitionFilePath string `json:"previous_definition_file_path" bson:"previous_definition_file_path,omitempty"`
	IsActive                   *bool  `json:"is_active" bson:"is_active,omitempty" binding:"required"`
	CreatedAt                  int    `json:"created_at" bson:"created_at,omitempty"`
	LastModifiedAt             int    `json:"last_modified_at" bson:"last_modified_at,omitempty"`
}

type DeviceDefinitionSerializer struct {
	ID             string                                      `json:"id"`
	Name           string                                      `json:"name" bson:"name"`
	DataFields     map[string]DeviceDynamicFieldDefinition     `json:"data_fields" bson:"data_fields"`
	Queries        map[string]DeviceDataFieldQueryDefinition   `json:"queries" bson:"queries"`
	Commands       map[string]DeviceDataFieldCommandDefinition `json:"commands" bson:"commands"`
	IsActive       bool                                        `json:"is_active" bson:"is_active"`
	CreatedAt      int                                         `json:"created_at" bson:"created_at"`
	LastModifiedAt int                                         `json:"last_modified_at" bson:"last_modified_at"`
}

func SerializeDeviceDefinition(def *DeviceDefinition) DeviceDefinitionSerializer {

	body := DeviceDataFieldsDefinition{}

	yamlFile, _ := os.ReadFile(def.DefinitionFilePath)
	js, _ := yaml.YAMLToJSON(yamlFile)
	json.Unmarshal(js, &body)

	serializedItem := DeviceDefinitionSerializer{
		ID:             def.ID,
		Name:           def.Name,
		DataFields:     body.DataFields,
		Queries:        body.Queries,
		Commands:       body.Commands,
		IsActive:       true,
		CreatedAt:      def.CreatedAt,
		LastModifiedAt: def.LastModifiedAt,
	}

	return serializedItem
}

func SerializeDeviceDefinitionsInBatch(defs *[]DeviceDefinition) []DeviceDefinitionSerializer {
	serializedItems := []DeviceDefinitionSerializer{}

	for _, def := range *defs {
		serializedItems = append(serializedItems, SerializeDeviceDefinition(&def))
	}

	return serializedItems
}

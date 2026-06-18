package models

import (
	. "v-gnms/pkg/core/database"
)

type Device struct {
	ID                  string                 `json:"id"`
	Name                string                 `json:"name" bson:"name,omitempty"`
	DeviceDefinitionID  string                 `json:"device_definition_id" bson:"device_definition_id,omitempty"`
	DataFields          map[string]interface{} `json:"data_fields" bson:"data_fields,omitempty"`
	IsActive            *bool                  `json:"is_active" bson:"is_active,omitempty" binding:"required"`
	LatestPollingStatus *int                   `json:"latest_polling_status" bson:"latest_polling_status,omitempty"`
	CreatedAt           int                    `json:"created_at" bson:"created_at,omitempty"`
	LastModifiedAt      int                    `json:"last_modified_at" bson:"last_modified_at,omitempty"`
}

type DeviceSerializer struct {
	ID                   string                 `json:"id"`
	Name                 string                 `json:"name" bson:"name"`
	DeviceDefinitionID   string                 `json:"device_definition_id" bson:"device_definition_id"`
	DeviceDefinitionName string                 `json:"device_definition_name" bson:"device_definition_name"`
	DataFields           map[string]interface{} `json:"data_fields" bson:"data_fields"`
	IsActive             bool                   `json:"is_active" bson:"is_active"`
	LatestPollingStatus  int                    `json:"latest_polling_status" bson:"latest_polling_status"`
	CreatedAt            int                    `json:"created_at" bson:"created_at"`
	LastModifiedAt       int                    `json:"last_modified_at" bson:"last_modified_at"`
}

func SerializeDevice(device *Device) DeviceSerializer {

	deviceDefinition, _ := FindSingleByID[DeviceDefinition](DeviceDefinitionsCollection, device.DeviceDefinitionID, nil)
	serializedDeviceDefinition := SerializeDeviceDefinition(&deviceDefinition)

	latestPollingStatus := 0
	if device.LatestPollingStatus != nil {
		latestPollingStatus = *device.LatestPollingStatus
	}

	serializedItem := DeviceSerializer{
		ID:                   device.ID,
		Name:                 device.Name,
		DeviceDefinitionID:   device.DeviceDefinitionID,
		DeviceDefinitionName: serializedDeviceDefinition.Name,
		DataFields:           device.DataFields,
		IsActive:             *device.IsActive,
		LatestPollingStatus:  latestPollingStatus,
		CreatedAt:            device.CreatedAt,
		LastModifiedAt:       device.LastModifiedAt,
	}

	for key, value := range serializedItem.DataFields {
		serializedItem.DataFields[key] = map[string]interface{}{
			"rules": serializedDeviceDefinition.DataFields[key],
			"value": value,
		}
	}

	return serializedItem
}

func SerializeDevicesInBatch(devices *[]Device) []DeviceSerializer {
	serializedItems := []DeviceSerializer{}

	for _, device := range *devices {
		serializedItems = append(serializedItems, SerializeDevice(&device))
	}

	return serializedItems
}

package models

import (
	. "v-gnms/pkg/core/database"
)

type DeviceReport struct {
	ID         string `json:"id" bson:"id"`
	DeviceID   string `json:"device_id" bson:"device_id,omitempty"`
	Type       *int   `json:"type" bson:"type,omitempty"`
	Level      *int   `json:"level" bson:"level,omitempty"`
	Info       string `json:"info" bson:"info,omitempty"`
	ReportedAt int    `json:"reported_at" bson:"reported_at,omitempty"`
}

type DeviceReportSerializer struct {
	ID         string `json:"id" bson:"id"`
	DeviceID   string `json:"device_id" bson:"device_id"`
	DeviceName string `json:"device_name" bson:"device_name"`
	Type       int    `json:"type" bson:"type"`
	Level      int    `json:"level" bson:"level"`
	Info       string `json:"info" bson:"info"`
	ReportedAt int    `json:"reported_at" bson:"reported_at"`
}

func SerializeDeviceReport(report *DeviceReport) DeviceReportSerializer {

	device, _ := FindSingleByID[Device](DevicesCollection, report.DeviceID, nil)

	serializedItem := DeviceReportSerializer{
		ID:         report.ID,
		DeviceID:   report.DeviceID,
		DeviceName: device.Name,
		Type:       *report.Type,
		Level:      *report.Level,
		Info:       report.Info,
		ReportedAt: report.ReportedAt,
	}

	return serializedItem
}

func SerializeDeviceReportsInBatch(reports *[]DeviceReport) []DeviceReportSerializer {
	serializedItems := []DeviceReportSerializer{}

	for _, report := range *reports {
		serializedItems = append(serializedItems, SerializeDeviceReport(&report))
	}

	return serializedItems
}

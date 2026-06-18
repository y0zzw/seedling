package models

type DevicePollingResult struct {
	ID       string `json:"id" bson:"id"`
	DeviceID string `json:"device_id" bson:"device_id,omitempty"`

	// 0: Disconnected, 1: Connected
	Status *int `json:"status" bson:"status,omitempty"`

	RequestedAt int `json:"requested_at" bson:"requested_at,omitempty"`
	RespondedAt int `json:"responded_at" bson:"responded_at,omitempty"`
}

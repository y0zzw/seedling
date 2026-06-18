package models

type WateringSchedule struct {
	ID        string `bson:"_id"        json:"id"`
	DeviceID  string `bson:"device_id"  json:"device_id"`
	StartDate string `bson:"start_date" json:"start_date"`
	PlantType int    `bson:"plant_type" json:"plant_type"`
	Schedule  []int  `bson:"schedule"   json:"schedule"`
	DayCount  int    `bson:"day_count"  json:"day_count"`
	CreatedAt int    `bson:"created_at" json:"created_at"`
}

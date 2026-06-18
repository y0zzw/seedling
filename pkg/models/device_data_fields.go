package models

type DeviceDynamicFieldDefinition struct {
	Type      string `json:"type" bson:"type,omitempty"`
	Min       int    `json:"min" bson:"min,omitempty"`
	Max       int    `json:"max" bson:"max,omitempty"`
	MaxLength int    `json:"maxlength" bson:"maxlength,omitempty"`
	Required  bool   `json:"required" bson:"required,omitempty"`
}

type DeviceDataFieldQueryDefinition struct {
	DownlinkFields map[string]DeviceDynamicFieldDefinition `json:"downlink_fields" bson:"downlink_fields,omitempty"`
}

type DeviceDataFieldCommandDefinition struct {
	DownlinkFields map[string]DeviceDynamicFieldDefinition `json:"downlink_fields" bson:"downlink_fields,omitempty"`
}

type DeviceDataFieldsDefinition struct {
	Name       string                                      `json:"display_name"`
	DataFields map[string]DeviceDynamicFieldDefinition     `json:"data_fields" bson:"data_fields,omitempty"`
	Queries    map[string]DeviceDataFieldQueryDefinition   `json:"query" bson:"queries,omitempty"`
	Commands   map[string]DeviceDataFieldCommandDefinition `json:"commands" bson:"commands,omitempty"`
}

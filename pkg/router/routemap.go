package router

import (
	"v-gnms/pkg/api"
	"v-gnms/pkg/core/rbac"

	"github.com/gin-gonic/gin"
)

type GinRoute struct {
	Identifier  string
	Path        string
	Method      string
	Handler     func(*gin.Context)
	DisplayName string
}

var ROUTE_MAP = []GinRoute{
	// {Identifier: "auth.login", Method: "POST", Path: "auth/login", Handler: api.Login, DisplayName: "Login"},
	// {Identifier: "auth.logout", Method: "POST", Path: "auth/logout", Handler: api.Logout, DisplayName: "Logout"},

	{Identifier: "device_definitions.list", Method: "GET", Path: "device-definitions", Handler: api.ListDeviceDefinitions, DisplayName: "List Device Definitions"},
	{Identifier: "device_definitions.get", Method: "GET", Path: "device-definition/:id", Handler: api.GetSingleDeviceDefinition, DisplayName: "Get Single Device Definition"},
	{Identifier: "device_definitions.get_file", Method: "GET", Path: "device-definition/:id/file", Handler: api.FetchDeviceDefinitionFileByID, DisplayName: "Get Device Definition File"},
	{Identifier: "device_definitions.create", Method: "POST", Path: "device-definitions", Handler: api.CreateDeviceDefinition, DisplayName: "Create Device Definition"},
	{Identifier: "device_definitions.update", Method: "PATCH", Path: "device-definition/:id", Handler: api.UpdateDeviceDefinition, DisplayName: "Update Device Definition"},
	{Identifier: "device_definitions.delete", Method: "DELETE", Path: "device-definition/:id", Handler: api.DeleteDeviceDefinition, DisplayName: "Delete Device Definition"},

	{Identifier: "devices.list", Method: "GET", Path: "devices", Handler: api.ListDevices, DisplayName: "List Devices"},
	{Identifier: "devices.get", Method: "GET", Path: "device/:id", Handler: api.GetSingleDevice, DisplayName: "Get Single Device"},
	{Identifier: "devices.create", Method: "POST", Path: "devices", Handler: api.CreateDevice, DisplayName: "Create Device"},
	{Identifier: "devices.update", Method: "PATCH", Path: "device/:id", Handler: api.UpdateDevice, DisplayName: "Update Device"},
	{Identifier: "devices.delete", Method: "DELETE", Path: "device/:id", Handler: api.DeleteDevice, DisplayName: "Delete Device"},

	{Identifier: "device_control.polling", Method: "POST", Path: "device-control/polling/:id", Handler: api.RequestDevicePolling, DisplayName: "Request Device Polling"},
	{Identifier: "device_control.polling.list", Method: "GET", Path: "device-control/polling", Handler: api.ListDevicePollingResults, DisplayName: "List Device Polling Results"},
	{Identifier: "device_control.polling.list_for_device", Method: "GET", Path: "device-control/polling/:device_id", Handler: api.ListDevicePollingResultsForDevice, DisplayName: "List Device Polling Results for Device"},
	{Identifier: "device_control.polling.delete", Method: "DELETE", Path: "device-control/polling/:id", Handler: api.DeleteDevicePollingResult, DisplayName: "Delete Device Polling Result"},

	{Identifier: "device_control.query", Method: "POST", Path: "device-control/query/:id", Handler: api.RequestDeviceQuery, DisplayName: "Request Device Query"},
	{Identifier: "device_control.command", Method: "POST", Path: "device-control/command/:id", Handler: api.RequestDeviceCommand, DisplayName: "Request Device Command"},

	{Identifier: "device_report.list", Method: "GET", Path: "device-report", Handler: api.ListDeviceReports, DisplayName: "List Device Report"},
	{Identifier: "device_report.list_for_device", Method: "GET", Path: "device-report/:device_id", Handler: api.ListDeviceReportsForDevice, DisplayName: "List Device Reports for Device"},
	// {Identifier: "device_report.get", Method: "GET", Path: "device-report/:id", Handler: api.GetSingleDeviceReport, DisplayName: "Get Single Device Report"},
	{Identifier: "device_report.delete", Method: "DELETE", Path: "device-report/:id", Handler: api.DeleteDeviceReport, DisplayName: "Delete Device Report"},
	// {Identifier: "device_report.get_device_uuid_by_device_id", Method: "GET", Path: "device-uuid/:device_id", Handler: api.GetDeviceUuidByDeviceID, DisplayName: "Get Device UUID By Device ID"},

	{Identifier: "roles.list", Method: "GET", Path: "roles", Handler: api.ListRoles, DisplayName: "List Roles"},
	{Identifier: "roles.get", Method: "GET", Path: "role/:id", Handler: api.GetSingleRole, DisplayName: "Get Single Role"},
	{Identifier: "roles.create", Method: "POST", Path: "roles", Handler: api.CreateRole, DisplayName: "Create Role"},
	{Identifier: "roles.update", Method: "PATCH", Path: "role/:id", Handler: api.UpdateRole, DisplayName: "Update Role"},
	{Identifier: "roles.delete", Method: "DELETE", Path: "role/:id", Handler: api.DeleteRole, DisplayName: "Delete Role"},
	{Identifier: "roles.actions.list", Method: "GET", Path: "roles/actions", Handler: api.GetRBACActions, DisplayName: "List Actions"},

	{Identifier: "users.list", Method: "GET", Path: "users", Handler: api.ListUsers, DisplayName: "List Users"},
	{Identifier: "users.get", Method: "GET", Path: "user/:id", Handler: api.GetSingleUser, DisplayName: "Get Single User"},
	{Identifier: "users.create", Method: "POST", Path: "users", Handler: api.CreateUser, DisplayName: "Create User"},
	{Identifier: "users.update", Method: "PATCH", Path: "user/:id", Handler: api.UpdateUser, DisplayName: "Update User"},
	{Identifier: "users.delete", Method: "DELETE", Path: "user/:id", Handler: api.DeleteUser, DisplayName: "Delete User"},
	// {Identifier: "watering_schedule.upload", Method: "POST", Path: "devices/:device_id/watering-schedule", Handler: api.UploadWateringSchedule, DisplayName: "Upload Watering Schedule"},
	// {Identifier: "watering_schedule.get", Method: "GET", Path: "devices/:device_id/watering-schedule", Handler: api.GetWateringSchedule, DisplayName: "Get Watering Schedule"},
}

func ExtractRBACActions() {
	results := []map[string]interface{}{}

	for _, route := range ROUTE_MAP {
		item := make(map[string]interface{})
		item["identifier"] = route.Identifier
		item["display_name"] = route.DisplayName
		results = append(results, item)
	}

	rbac.RBACActions = results

	rbac.CheckAndInitializeSuperuserPermissions()
}

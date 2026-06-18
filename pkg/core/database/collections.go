package database

import (
	// . "v-gnms/pkg/models"

	"go.mongodb.org/mongo-driver/mongo"
)

var Database *mongo.Database

var DeviceDefinitionsCollection *mongo.Collection
var DevicesCollection *mongo.Collection
var DevicePollingResultsCollection *mongo.Collection
var DeviceReportsCollection *mongo.Collection
var RolesCollection *mongo.Collection
var UsersCollection *mongo.Collection

// var WateringSchedulesCollection = GetCollection("watering_schedules")
var WateringSchedulesCollection *mongo.Collection

// var RolesCollectionX *mongox.Collection[Role]

// var UsersCollectionX *mongox.Collection[User]

// Initialize the database and collections
// This is called only upon MongoDB initialization
// and should not be called by external modules
func initializeDatabaseAndCollections(mongoClient *mongo.Client) {
	Database = mongoClient.Database("vgnms")

	DeviceDefinitionsCollection = Database.Collection("device_definitions")
	DevicesCollection = Database.Collection("devices")
	DevicePollingResultsCollection = Database.Collection("device_polling_results")
	DeviceReportsCollection = Database.Collection("device_reports")
	RolesCollection = Database.Collection("roles")
	UsersCollection = Database.Collection("users")
	WateringSchedulesCollection = Database.Collection("watering_schedules")
	// RolesCollectionX = mongox.NewCollection[Role](RolesCollection)
	// UsersCollectionX = mongox.NewCollection[User](UsersCollection)
}

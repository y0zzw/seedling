package database

import (
	"context"
	"sync"
	cfg "v-gnms/config"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoClient *mongo.Client
var once sync.Once

// Get singleton MongoDB client instance
// This initializes the connection for the first time
func GetMongoClient() *mongo.Client {
	once.Do(func() {
		mongoOptions := options.Client().ApplyURI(cfg.GetConfigValue("MONGODB_URI"))

		var err error

		// Connect to the MongoDB instance
		mongoClient, err = mongo.Connect(context.TODO(), mongoOptions)
		if err != nil {
			log.Fatal().Err(err)
		}

		// Attempt to ping MongoDB to ensure a valid connection
		err = mongoClient.Ping(context.TODO(), nil)
		if err != nil {
			log.Fatal().Err(err)
		}

		// Initialize database and collections
		initializeDatabaseAndCollections(mongoClient)

		log.Info().Msg("MongoDB initialization complete.")
	})

	return mongoClient
}

package database

import (
	"time"

	"github.com/google/uuid"
)

func GenerateUUIDv7() string {
	result, _ := uuid.NewV7()
	return result.String()
}

func GetCurrentTimestampSeconds() int {
	return int(time.Now().Unix())
}

package database

import (
	"context"
	"reflect"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// FindMultiple retrieves multiple documents from a collection based on filter and sort options.
func FindMultiple[T any](collection *mongo.Collection, filterOptions interface{}, findOptions *options.FindOptions) ([]T, int, error) {
	var results []T

	// First, count the number of documents satisfying the filter conditions
	count, err := collection.CountDocuments(context.TODO(), filterOptions)
	if err != nil {
		return nil, 0, err
	}

	// If the count is zero, there is no need to find anymore.
	if count == 0 {
		return []T{}, 0, nil
	}

	// Find the items with the find options (skip, limit, sorting, etc.)
	cursor, err := collection.Find(context.TODO(), filterOptions, findOptions)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var elem T
		if err := cursor.Decode(&elem); err != nil {
			return nil, 0, err
		}
		results = append(results, elem)
	}

	if err := cursor.Err(); err != nil {
		return nil, 0, err
	}

	return results, int(count), nil
}

func Count[T any](collection *mongo.Collection, filterOptions interface{}, findOptions *options.CountOptions) (int, error) {
	count, err := collection.CountDocuments(context.TODO(), filterOptions, findOptions)
	if err != nil {
		return -1, err
	}
	return int(count), nil
}

// FindSingleByID retrieves a single document from a collection based on its ID.
func FindSingleByID[T any](collection *mongo.Collection, id string, options *options.FindOneOptions) (T, error) {
	var result T
	filter := bson.M{"id": id}

	err := collection.FindOne(context.TODO(), filter, options).Decode(&result)
	if err != nil {
		return result, err
	}

	return result, nil
}

// FindSingleByID retrieves a single document from a collection based on its ID.
func FindSingleByField[T any](collection *mongo.Collection, field string, value any, options *options.FindOneOptions) (T, error) {
	var result T
	filter := bson.M{field: value}

	err := collection.FindOne(context.TODO(), filter, options).Decode(&result)
	if err != nil {
		return result, err
	}

	return result, nil
}

// Create inserts a new document into the collection and returns the created object.
func Create[T any](collection *mongo.Collection, item T) (T, error) {
	SetValueOfAnyTypedObject(&item, "CreatedAt", GetCurrentTimestampSeconds())
	SetValueOfAnyTypedObject(&item, "LastModifiedAt", GetCurrentTimestampSeconds())

	_, err := collection.InsertOne(context.TODO(), item)
	if err != nil {
		return item, err
	}
	return item, nil
}

// Update modifies a document in the collection based on its ID and returns the updated object.
func Update[T any](collection *mongo.Collection, id string, newValues T) (T, error) {
	var updatedDoc T
	filter := bson.M{"id": id}
	update := bson.M{"$set": newValues}

	opts := options.FindOneAndUpdateOptions{}
	opts.SetReturnDocument(options.After)

	err := collection.FindOneAndUpdate(context.TODO(), filter, update, &opts).Decode(&updatedDoc)
	if err != nil {
		return updatedDoc, err
	}

	return updatedDoc, nil
}

// Delete removes a document from the collection based on its ID and returns the deleted object.
func Delete[T any](collection *mongo.Collection, id string) (T, error) {
	var deletedDoc T
	filter := bson.M{"id": id}

	err := collection.FindOneAndDelete(context.TODO(), filter).Decode(&deletedDoc)
	if err != nil {
		return deletedDoc, err
	}

	return deletedDoc, nil
}

func SetValueOfAnyTypedObject(obj any, field string, value any) {
	ref := reflect.ValueOf(obj)

	// if its a pointer, resolve its value
	if ref.Kind() == reflect.Ptr {
		ref = reflect.Indirect(ref)
	}

	if ref.Kind() == reflect.Interface {
		ref = ref.Elem()
	}

	// should double check we now have a struct (could still be anything)
	if ref.Kind() != reflect.Struct {
		log.Error().Msg("Wrong type")
	}

	prop := ref.FieldByName(field)

	// Ref: https://stackoverflow.com/a/69750603
	// If the field does not exist, Value.FieldByName() returns the zero value of reflect.Value
	// so if prop is equal to a blank reflect.Value{}, skip it
	if prop == (reflect.Value{}) {
		return
	}

	prop.Set(reflect.ValueOf(value))
}

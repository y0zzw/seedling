package api

import (
	"net/http"

	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/core/rbac"
	. "v-gnms/pkg/core/utils"
	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/crypto/bcrypt"
)

func ListUsers(c *gin.Context) {
	queryParams := ExtractApiListQueryParams(c)

	users, itemCountInDB, err := FindMultiple[User](UsersCollection, bson.D{}, GenerateApiListOptions(queryParams))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serializedItems := SerializeUsersInBatch(&users)

	responsePayload := ConstructListResponsePayload(serializedItems, itemCountInDB)

	c.JSON(http.StatusOK, responsePayload)
}

func GetSingleUser(c *gin.Context) {
	id := c.Param("id")

	user, err := FindSingleByID[User](UsersCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serializedItem := SerializeUser(&user)

	responsePayload := ConstructGetSingleResponsePayload(serializedItem)
	c.JSON(http.StatusOK, responsePayload)
}

func CreateUser(c *gin.Context) {
	var item User

	// Returns [400] if data is malformed
	if err := c.ShouldBindJSON(&item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Check email uniquesness
	// Returns [409] if the email is already taken
	existingUserCountWithSameEmail, _ := Count[User](UsersCollection, bson.E{Key: "email", Value: item.Email}, nil)
	if existingUserCountWithSameEmail > 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "Email address already taken."})
		return
	}

	item.ID = GenerateUUIDv7()

	// Returns [400] if the password is not provided
	if len(item.Password) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Password not provided."})
		return
	}

	// Returns [400] if the password is provided but is too short
	if len(item.Password) < 8 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Password length too short."})
		return
	}

	// Hash and update the password
	hashed, _ := bcrypt.GenerateFromPassword([]byte(item.Password), 12)
	item.Password = string(hashed)

	// Find the role
	role, err := FindSingleByID[Role](RolesCollection, item.RoleID, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	item.RoleID = role.ID

	// Insert the user into DB
	user, err := Create[User](UsersCollection, item)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	SetUserRole(&user, &role)

	responsePayload := ConstructGetSingleResponsePayload(user)
	c.JSON(http.StatusOK, responsePayload)
}

func UpdateUser(c *gin.Context) {
	id := c.Param("id")

	var newItem User

	if err := c.ShouldBindJSON(&newItem); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Find the target item
	// Returns [404] if not found
	existingItem, err := FindSingleByID[User](UsersCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User not found."})
	}

	// Rehash the password, if it is provided
	if len(newItem.Password) > 0 {
		// Returns [400] if the password is provided but is too short
		if len(newItem.Password) < 8 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Password length too short."})
			return
		}
		hashed, _ := bcrypt.GenerateFromPassword([]byte(newItem.Password), 12)
		newItem.Password = string(hashed)
	}

	// Prevent the ID from being modified
	newItem.ID = existingItem.ID

	// Merge the new values into the existing item
	newItem = MergeStructs(existingItem, newItem)

	// Find the role
	role, err := FindSingleByID[Role](RolesCollection, newItem.RoleID, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	log.Info().Any("New User", newItem.RoleID).Msg("")

	// Update to DB
	updatedItem, err := Update[User](UsersCollection, id, newItem)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	SetUserRole(&updatedItem, &role)

	responsePayload := ConstructGetSingleResponsePayload(updatedItem)
	c.JSON(http.StatusOK, responsePayload)
}

func DeleteUser(c *gin.Context) {
	id := c.Param("id")

	user, err := Delete[User](UsersCollection, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	DeleteUserRole(&user)

	responsePayload := ConstructGetSingleResponsePayload(user)
	c.JSON(http.StatusOK, responsePayload)
}

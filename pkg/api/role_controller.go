package api

import (
	"net/http"

	. "v-gnms/pkg/core/database"
	"v-gnms/pkg/core/rbac"
	. "v-gnms/pkg/core/utils"
	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

func ListRoles(c *gin.Context) {
	queryParams := ExtractApiListQueryParams(c)

	roles, itemCountInDB, err := FindMultiple[Role](RolesCollection, bson.D{}, GenerateApiListOptions(queryParams))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructListResponsePayload(roles, itemCountInDB)
	c.JSON(http.StatusOK, responsePayload)
}

func GetSingleRole(c *gin.Context) {
	id := c.Param("id")

	role, err := FindSingleByID[Role](RolesCollection, id, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(role)
	c.JSON(http.StatusOK, responsePayload)
}

func CreateRole(c *gin.Context) {
	var item Role

	if err := c.ShouldBindJSON(&item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	item.ID = GenerateUUIDv7()

	role, err := Create[Role](RolesCollection, item)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(role)
	c.JSON(http.StatusOK, responsePayload)
}

func UpdateRole(c *gin.Context) {
	id := c.Param("id")

	var item Role

	if err := c.ShouldBindJSON(&item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	updatedItem, err := Update[Role](RolesCollection, id, item)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(updatedItem)
	c.JSON(http.StatusOK, responsePayload)
}

func DeleteRole(c *gin.Context) {
	id := c.Param("id")

	role, err := Delete[Role](RolesCollection, id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	responsePayload := ConstructGetSingleResponsePayload(role)
	c.JSON(http.StatusOK, responsePayload)
}

func GetRBACActions(c *gin.Context) {
	c.JSON(http.StatusOK, rbac.RBACActions)
}

package api

import (
	"net/http"
	"strings"

	. "v-gnms/pkg/core/database"
	. "v-gnms/pkg/core/jwt"
	. "v-gnms/pkg/models"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"
)

type LoginDataStruct struct {
	Email    string `json:"email" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func Login(c *gin.Context) {

	// Retrieve login data
	// Returns [400] if data is malformed
	var loginData LoginDataStruct
	if err := c.ShouldBindJSON(&loginData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Find the user by the supplied email address
	// Returns [401] if the user is not found
	user, err := FindSingleByField[User](UsersCollection, "email", loginData.Email, nil)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}

	// Verify the supplied password
	// Returns [401] upon mismatch
	encryptionErr := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(loginData.Password))
	if encryptionErr != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}

	// Token Parser, Create a new token if token is invalid, suppose to be expired or empty for first create
	if _, err := ParseJWTToken(user.Token); err != nil || user.Token == "" {
		token, err := CreateJWTToken(user.ID, user.Email)
		if err != nil {
			log.Err(err).Msg("")

			c.JSON(http.StatusInternalServerError, gin.H{"message": "Please contact system administator"})
			c.Abort()
			return
		}

		// Update the user token to DB
		user.Token = *token
		user, err = Update[User](UsersCollection, user.ID, user)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Please contact system administator"})
			c.Abort()
		}

	}
	// logger.NewInfo(logger.UserSessions, fmt.Sprintf("User %s has logged in. ", loginInput.Email))
	c.JSON(http.StatusOK, gin.H{"token": user.Token})
}

func Logout(c *gin.Context) {

	token := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")

	user, err := FindSingleByField[User](UsersCollection, "token", token, nil)
	if err != nil {
		log.Err(err).Msg("")
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Please contact system administator"})
		c.Abort()
		return
	}

	user.Token = "null"
	_, err = Update[User](UsersCollection, user.ID, user)
	if err != nil {
		log.Err(err).Msg("")
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Please contact system administator"})
		c.Abort()
		return
	}
	// logger.NewInfo(logger.UserSessions, fmt.Sprintf("User %s has logged out. ", loggedOutEmail))

	c.JSON(http.StatusOK, gin.H{"message": "Logout successfully"})
}

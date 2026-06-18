package models

import (
	. "v-gnms/pkg/core/database"
)

type User struct {
	ID       string `json:"id"`
	Email    string `json:"email" bson:"email,omitempty" binding:"required"`
	Password string `json:"password" bson:"password,omitempty"`

	Username  string `json:"username" bson:"username,omitempty" binding:"required"`
	FirstName string `json:"first_name" bson:"first_name,omitempty" binding:"required"`
	LastName  string `json:"last_name" bson:"last_name,omitempty" binding:"required"`
	RoleID    string `json:"role_id" bson:"role_id,omitempty" binding:"required"`
	IsActive  *bool  `json:"is_active" bson:"is_active,omitempty" binding:"required"`

	Token string `json:"token" bson:"token,omitempty"`

	CreatedAt      int `json:"created_at" bson:"created_at,omitempty"`
	LastModifiedAt int `json:"last_modified_at" bson:"last_modified_at,omitempty"`
}

type UserSerializer struct {
	ID    string `json:"id" bson:"id"`
	Email string `json:"email" bson:"email"`

	Username  string `json:"username" bson:"username"`
	FirstName string `json:"first_name" bson:"first_name"`
	LastName  string `json:"last_name" bson:"last_name"`

	RoleID   string `json:"role_id" bson:"role_id"`
	RoleName string `json:"role_name" bson:"role_name"`

	IsActive bool `json:"is_active" bson:"is_active"`

	CreatedAt      int `json:"created_at" bson:"created_at"`
	LastModifiedAt int `json:"last_modified_at" bson:"last_modified_at"`
}

func SerializeUser(user *User) UserSerializer {
	role, _ := FindSingleByID[Role](RolesCollection, user.RoleID, nil)

	serializedItem := UserSerializer{
		ID:             user.ID,
		Email:          user.Email,
		Username:       user.Username,
		FirstName:      user.FirstName,
		LastName:       user.LastName,
		RoleID:         user.RoleID,
		RoleName:       role.DisplayName,
		IsActive:       *user.IsActive,
		CreatedAt:      user.CreatedAt,
		LastModifiedAt: user.LastModifiedAt,
	}

	return serializedItem
}

func SerializeUsersInBatch(users *[]User) []UserSerializer {
	serializedItems := []UserSerializer{}

	for _, user := range *users {
		serializedItems = append(serializedItems, SerializeUser(&user))
	}

	return serializedItems
}

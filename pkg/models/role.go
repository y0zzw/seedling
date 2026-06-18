package models

type Role struct {
	ID             string `json:"id" bson:"id,omitempty"`
	IdentName      string `json:"ident_name" bson:"ident_name,omitempty" binding:"required"`
	DisplayName    string `json:"display_name" bson:"display_name,omitempty" binding:"required"`
	IsActive       *bool  `json:"is_active" bson:"is_active,omitempty" binding:"required"`
	CreatedAt      int    `json:"created_at" bson:"created_at,omitempty"`
	LastModifiedAt int    `json:"last_modified_at" bson:"last_modified_at,omitempty"`
}

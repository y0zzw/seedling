package jwt

import (
	"time"

	cfg "v-gnms/config"

	"github.com/golang-jwt/jwt/v5"
)

type JWTData struct {
	UID   string
	Email string
	jwt.RegisteredClaims
}

func CreateJWTToken(userId string, email string) (*string, error) {

	claims := JWTData{
		userId,
		email,
		jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(time.Now()),                     // iat
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour * 24)), // exp
			NotBefore: jwt.NewNumericDate(time.Now()),                     // nbf
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	signedToken, err := token.SignedString([]byte(cfg.GetConfigValue("JWT_SECRET")))

	if err != nil {
		return nil, err
	}

	return &signedToken, nil
}

func ParseJWTToken(tokenString string) (*JWTData, error) {

	// https://pkg.go.dev/github.com/golang-jwt/jwt/v5#WithLeeway
	token, err := jwt.ParseWithClaims(tokenString, &JWTData{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(cfg.GetConfigValue("JWT_SECRET")), nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithLeeway(time.Second*5))

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*JWTData); ok && token.Valid && err == nil {
		return claims, nil
	}

	return nil, err

}

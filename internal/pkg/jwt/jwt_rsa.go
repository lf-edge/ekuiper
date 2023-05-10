package jwt

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt"
)

const ExpireTimeMinutes = 10

type Token struct {
	jwt.StandardClaims
}

type ErrorType int8

const (
	JWT_VALIDATE_ERROR ErrorType = 1
)

const JWT_VALIDATE_TEMP = "JWTVAL__ERRCODE:%d__ERRSTR:%s__TOKEN:%s"
const JWT_OTHER_TEMP = "JWTOTH__ERRSTR:%s__TOKEN:%s"

type Error struct {
	errType   ErrorType
	Inner     error
	metaToken string
}

func (e Error) Error() string {
	switch e.errType {
	case JWT_VALIDATE_ERROR:
		if ve, ok := e.Inner.(*jwt.ValidationError); ok {
			return fmt.Sprintf(JWT_VALIDATE_TEMP, ve.Errors, ve.Error(), e.metaToken)
		}
	default:
		return fmt.Sprintf(JWT_OTHER_TEMP, e.Inner.Error(), e.metaToken)
	}
	return "Invalid token " + e.metaToken
}

func CreateToken(signKeyName, issuer, aud string) (string, error) {
	tk := &Token{}
	tk.Issuer = issuer
	tk.Audience = aud
	tk.ExpiresAt = time.Now().Add(time.Duration(ExpireTimeMinutes) * time.Minute).Unix()
	token := jwt.NewWithClaims(jwt.GetSigningMethod("RS256"), tk)
	signKey, err := GetPrivateKeyWithKeyName(signKeyName)
	if err != nil {
		return "", err
	}
	return token.SignedString(signKey)
}

func ParseToken(th string) (*Token, error) {
	tk := &Token{}
	token, err := jwt.ParseWithClaims(th, tk, func(token *jwt.Token) (interface{}, error) {
		jwtToken := token.Claims.(*Token)

		if jwtToken.Issuer == "" {
			return "", fmt.Errorf("issuer field not exist in jwt payload")
		}
		pubKey, err := GetPublicKey(jwtToken.Issuer)
		if err != nil {
			return "", err
		}
		return pubKey, nil
	})
	if ve, ok := err.(*jwt.ValidationError); ok {
		return tk, Error{
			errType:   JWT_VALIDATE_ERROR,
			Inner:     ve,
			metaToken: th,
		}
	}

	if err != nil {
		return nil, err
	}
	if !token.Valid {
		return nil, errors.New("invalid token")
	}
	return tk, nil
}

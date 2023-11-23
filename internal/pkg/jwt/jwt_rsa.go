// Copyright 2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jwt

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const ExpireTimeMinutes = 10

type Token struct {
	jwt.RegisteredClaims
}

// CreateToken Only for tests
func CreateToken(signKeyName, issuer string, aud []string) (string, error) {
	tk := &Token{}
	tk.Issuer = issuer
	tk.Audience = aud
	tk.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Duration(ExpireTimeMinutes) * time.Minute))
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
	if err != nil {
		return tk, fmt.Errorf("validate token error: %s", err)
	}
	if !token.Valid {
		return nil, errors.New("invalid token")
	}
	return tk, nil
}

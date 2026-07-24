// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"crypto/rsa"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

const RSAKeyDir = "mgmt"

func GetPublicKey(keyName string) (*rsa.PublicKey, error) {
	return publicKeyFromFile(keyName)
}

func insensitiveGetFilePath(keyName string) (string, error) {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return "", err
	}

	dir := path.Join(confDir, RSAKeyDir)
	dirEntries, err := os.ReadDir(dir)
	if nil != err {
		return "", err
	}

	for _, entry := range dirEntries {
		fileName := entry.Name()
		if strings.EqualFold(fileName, keyName) {
			filePath := path.Join(dir, fileName)
			return filePath, nil
		}
	}
	return "", fmt.Errorf("not found target key file %s in /etc/%s", keyName, RSAKeyDir)
}

func publicKeyFromFile(keyName string) (*rsa.PublicKey, error) {
	keyPath, err := insensitiveGetFilePath(keyName)
	if err != nil {
		return nil, err
	}
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	pubKey, err := jwt.ParseRSAPublicKeyFromPEM(keyBytes)
	if err != nil {
		return nil, err
	}
	return pubKey, nil
}

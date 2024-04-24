// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

package encryptor

import (
	"crypto/rand"
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/encryptor/aes"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

func GetEncryptor(name string) (message.Encryptor, error) {
	if name == "aes" {
		key, iv := getKeyIv()
		return aes.NewStreamEncrypter(key, iv)
	}
	return nil, fmt.Errorf("unsupported encryptor: %s", name)
}

func GetEncryptWriter(name string, output io.Writer) (io.Writer, error) {
	if name == "aes" {
		key, iv := getKeyIv()
		return aes.NewStreamWriter(key, iv, output)
	}
	return nil, fmt.Errorf("unsupported encryptor: %s", name)
}

func getKeyIv() ([]byte, []byte) {
	key := conf.Config.AesKey
	iv := make([]byte, 16)
	// Use the crypto/rand package to generate random bytes
	_, _ = rand.Read(iv)
	return key, iv
}

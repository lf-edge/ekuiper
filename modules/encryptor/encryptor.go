// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/v2/modules/encryptor/aes"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

func GetEncryptor(name string, key []byte, encryptProps map[string]any) (message.Encryptor, error) {
	switch name {
	case "aes":
		return aes.GetEncryptor(key, encryptProps)
	case "default":
		d, err := GetDefaultDecryptor()
		if err != nil {
			return nil, err
		} else {
			return d.(message.Encryptor), nil
		}
	default:
		return nil, fmt.Errorf("encryptor '%s' is not supported", name)
	}
}

func GetEncryptWriter(name string, output io.Writer, key []byte) (io.Writer, error) {
	// TODO support encryption props later
	if name == "aes" {
		return aes.GetEncryptWriter(output, key, nil)
	}
	return nil, fmt.Errorf("unsupported encryptor: %s", name)
}

func GetDecryptor(name string, key []byte, decryptProps map[string]any) (message.Decryptor, error) {
	switch name {
	case "aes":
		return aes.GetDecryptor(key, decryptProps)
	case "default":
		return GetDefaultDecryptor()
	default:
		return nil, fmt.Errorf("decryptor '%s' is not supported", name)
	}
}

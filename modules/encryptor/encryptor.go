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
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// GetEncryptor currently, decryptor and encryptor are the same instance
func GetEncryptor(name string, encryptProps map[string]any, conf *model.KuiperConf) (message.Encryptor, error) {
	var (
		key []byte
		err error
	)
	switch name {
	case "aes":
		key, err = getAESKey(conf)
		if err != nil {
			return nil, err
		}
	}
	enc, err := GetDecryptorWithKey(name, key, encryptProps)
	if err != nil {
		return nil, err
	}
	return enc.(message.Encryptor), nil
}

func GetDecryptorWithKey(name string, key []byte, encryptProps map[string]any) (message.Decryptor, error) {
	switch name {
	case "aes":
		return aes.GetDecryptor(key, encryptProps)
	case "default":
		d, err := GetDefaultDecryptor()
		if err != nil {
			return nil, err
		} else {
			return d, nil
		}
	default:
		return nil, fmt.Errorf("encryptor '%s' is not supported", name)
	}
}

func getAESKey(conf *model.KuiperConf) ([]byte, error) {
	if conf != nil && conf.AesKey != nil {
		return conf.AesKey, nil
	}
	return nil, fmt.Errorf("AES Key is not defined")
}

func GetEncryptWriter(name string, output io.Writer, conf *model.KuiperConf) (io.Writer, error) {
	// TODO support encryption props later
	if name == "aes" {
		key, err := getAESKey(conf)
		if err != nil {
			return nil, err
		}
		return aes.GetEncryptWriter(output, key, nil)
	}
	return nil, fmt.Errorf("unsupported encryptor: %s", name)
}

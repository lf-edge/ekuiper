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

package aes

import (
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type c struct {
	Mode    string `json:"mode"`
	Iv      string `json:"iv"`
	Aad     string `json:"aad"`
	TagSize int    `json:"tagsize"`
}

func GetEncryptor(key []byte, props map[string]any) (message.Encryptor, error) {
	cc := &c{Mode: "cfb"}
	err := cast.MapToStruct(props, cc)
	if err != nil {
		return nil, err
	}
	switch cc.Mode {
	case "cfb":
		return NewStreamEncrypter(key, cc)
	case "gcm":
		return NewGcmEncrypter(key, cc)
	default:
		return nil, fmt.Errorf("unsupported AES encryption mode: %s", cc.Mode)
	}
}

func GetEncryptWriter(output io.Writer, key []byte, props map[string]any) (io.Writer, error) {
	cc := &c{Mode: "cfb"}
	err := cast.MapToStruct(props, cc)
	if err != nil {
		return nil, err
	}
	switch cc.Mode {
	case "cfb":
		return NewStreamWriter(key, output, cc)
	default:
		return nil, fmt.Errorf("unsupported AES writer mode: %s", cc.Mode)
	}
}

func GetDecryptor(key []byte, props map[string]any) (message.Decryptor, error) {
	cc := &c{Mode: "gcm"}
	err := cast.MapToStruct(props, cc)
	if err != nil {
		return nil, err
	}
	switch cc.Mode {
	case "cfb":
		return NewStreamEncrypter(key, cc)
	case "gcm":
		return NewGcmEncrypter(key, cc)
	default:
		return nil, fmt.Errorf("unsupported AES encryption mode: %s", cc.Mode)
	}
}

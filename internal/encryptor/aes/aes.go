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

package aes

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
)

type StreamEncrypter struct {
	block cipher.Block
	iv    []byte
}

func (a *StreamEncrypter) Encrypt(data []byte) []byte {
	ciphertext := make([]byte, len(data))
	stream := cipher.NewCFBEncrypter(a.block, a.iv)
	stream.XORKeyStream(ciphertext, data)
	result := append(a.iv, ciphertext...)
	return result
}

func NewStreamEncrypter(key, iv []byte) (*StreamEncrypter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return &StreamEncrypter{
		block: block,
		iv:    iv,
	}, nil
}

func NewStreamWriter(key, iv []byte, output io.Writer) (*cipher.StreamWriter, error) {
	blockMode, err := newAesStream(key, iv)
	if err != nil {
		return nil, err
	}
	writer := &cipher.StreamWriter{S: blockMode, W: output}
	_, err = writer.W.Write(iv)
	if err != nil {
		return nil, fmt.Errorf("failed to write iv: %v", err)
	}
	return writer, nil
}

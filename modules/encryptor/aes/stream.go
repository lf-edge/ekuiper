// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
)

type StreamEncrypter struct {
	block      cipher.Block
	constantIv []byte
}

func (a *StreamEncrypter) Encrypt(data []byte) ([]byte, error) {
	iv := a.constantIv
	if iv == nil {
		iv = make([]byte, 16)
		// Use the crypto/rand package to generate random bytes
		_, err := rand.Read(iv)
		if err != nil {
			return nil, err
		}
	}
	ciphertext := make([]byte, len(data))
	stream := cipher.NewCFBEncrypter(a.block, iv) //nolint:staticcheck
	stream.XORKeyStream(ciphertext, data)
	result := append(iv, ciphertext...)
	return result, nil
}

func (a *StreamEncrypter) Decrypt(secret []byte) ([]byte, error) {
	iv := a.constantIv
	if iv == nil {
		iv = secret[:aes.BlockSize]
	}
	secret = secret[aes.BlockSize:]
	stream := cipher.NewCFBDecrypter(a.block, iv) //nolint:staticcheck
	revert := make([]byte, len(secret))
	stream.XORKeyStream(revert, secret)
	return revert, nil
}

func NewStreamEncrypter(key []byte, cc *c) (*StreamEncrypter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	enc := &StreamEncrypter{
		block: block,
	}
	if cc.Iv != "" {
		iv, err := base64.StdEncoding.DecodeString(cc.Iv)
		if err != nil {
			return nil, fmt.Errorf("invalid IV setting")
		}
		if len(iv) != 16 {
			return nil, fmt.Errorf("invalid IV length")
		}
		enc.constantIv = iv
	}
	return enc, nil
}

func NewStreamWriter(key []byte, output io.Writer, cc *c) (*cipher.StreamWriter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("Error creating AES cipher block: %v", err)
	}
	var iv []byte
	if cc.Iv != "" {
		iv, err = hex.DecodeString(cc.Iv)
		if err != nil {
			return nil, fmt.Errorf("invalid IV setting")
		}
		if len(iv) != 16 {
			return nil, fmt.Errorf("invalid IV length")
		}
	} else {
		iv = make([]byte, 16)
		_, err = rand.Read(iv)
		if err != nil {
			return nil, err
		}
	}
	blockMode := cipher.NewCFBEncrypter(block, iv) //nolint:staticcheck
	writer := &cipher.StreamWriter{S: blockMode, W: output}
	// Only add iv when it is not constant
	if cc.Iv == "" {
		_, err = writer.W.Write(iv)
		if err != nil {
			return nil, fmt.Errorf("failed to write iv: %v", err)
		}
	}
	return writer, nil
}

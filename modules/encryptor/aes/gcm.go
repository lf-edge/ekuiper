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
	"fmt"
	"io"
)

type GcmEncrypter struct {
	gcm           cipher.AEAD
	constantNonce []byte
	aad           []byte
	tagSize       int
}

func NewGcmEncrypter(key []byte, cc *c) (*GcmEncrypter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	enc := &GcmEncrypter{
		gcm: gcm,
	}
	if cc.Iv != "" {
		iv, err := base64.StdEncoding.DecodeString(cc.Iv)
		if err != nil {
			return nil, fmt.Errorf("invalid IV setting")
		}
		if len(iv) != gcm.NonceSize() {
			return nil, fmt.Errorf("invalid IV length")
		}
		enc.constantNonce = iv
	}
	if cc.Aad != "" {
		aad, err := base64.StdEncoding.DecodeString(cc.Aad)
		if err != nil {
			return nil, fmt.Errorf("invalid Aad setting")
		}
		enc.aad = aad
	}
	if cc.TagSize > 16 {
		enc.tagSize = cc.TagSize
	}
	return enc, nil
}

func (a *GcmEncrypter) Encrypt(data []byte) ([]byte, error) {
	nonceSize := a.gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("cipertext too short")
	}
	nonce := a.constantNonce
	if nonce == nil {
		nonce = make([]byte, a.gcm.NonceSize())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return nil, err
		}
	}
	if a.aad == nil {
		ciphertext := a.gcm.Seal(nonce, nonce, data, a.aad)
		return ciphertext, nil
	} else {
		ciphertext := a.gcm.Seal(nil, nonce, data, a.aad)
		tagSize := a.gcm.Overhead()
		tag := ciphertext[len(ciphertext)-tagSize:]
		ciphertext = ciphertext[:len(ciphertext)-tagSize]
		if a.tagSize > 0 {
			padding := a.tagSize - len(tag)
			if padding > 0 {
				tag = append(tag, make([]byte, padding)...)
			}
		}
		return append(tag, ciphertext...), nil
	}
}

func (a *GcmEncrypter) Decrypt(secret []byte) ([]byte, error) {
	nonceSize := a.gcm.NonceSize()
	if len(secret) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce := a.constantNonce
	if nonce == nil {
		nonce, secret = secret[:nonceSize], secret[nonceSize:]
	}
	ciphertext := secret
	if a.tagSize > 0 {
		tag := secret[:16]
		secret = secret[32:]
		ciphertext = append(secret, tag...)
	}
	return a.gcm.Open(nil, nonce, ciphertext, a.aad)
}

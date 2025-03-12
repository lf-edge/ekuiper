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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func TestMessage(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{
			name: "cfb",
			mode: "cfb",
		},
		{
			name: "gcm",
			mode: "gcm",
		},
	}
	key := []byte("0123456789abcdef0123456789abcdef")
	// plaintext
	pt := "Using the Input type selection, choose the type of input – a text string or a file. In case of the text string input, enter your input into the Input text textarea1,2. Otherwise, use the \"Browse\" button to select the input file to upload. Then select the cryptographic function you want to use in the Function field. Depending on the selected function the Initialization vector (IV) field is shown or hidden. Initialization vector is always a sequence of bytes, each byte has to be represented in hexadecimal form."
	if conf.Config == nil {
		conf.Config = &conf.KuiperConf{}
	}
	conf.Config.AesKey = key
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, err := GetEncryptor(map[string]any{"mode": tt.mode})
			assert.NoError(t, err)

			// encrypt once
			secret, err := enc.Encrypt([]byte(pt))
			assert.NoError(t, err)
			// decrypt once
			revert, err := decrypt(tt.mode, key, secret)
			assert.NoError(t, err)
			assert.Equal(t, pt, string(revert))
			// encrypt again
			secret, err = enc.Encrypt([]byte(pt))
			assert.NoError(t, err)
			// decrypt again
			revert, err = decrypt(tt.mode, key, secret)
			assert.NoError(t, err)
			assert.Equal(t, pt, string(revert))
		})
	}
}

func TestConfGCM(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	ivb := []byte("0123456789ab")
	iv := base64.StdEncoding.EncodeToString(ivb)
	addb := []byte("helloworld")
	aad := base64.StdEncoding.EncodeToString(addb)

	// plaintext
	pt := "Using the Input type selection, choose the type of input – a text string or a file. In case of the text string input, enter your input into the Input text textarea1,2. Otherwise, use the \"Browse\" button to select the input file to upload. Then select the cryptographic function you want to use in the Function field. Depending on the selected function the Initialization vector (IV) field is shown or hidden. Initialization vector is always a sequence of bytes, each byte has to be represented in hexadecimal form."
	if conf.Config == nil {
		conf.Config = &conf.KuiperConf{}
	}
	conf.Config.AesKey = key
	enc, err := GetEncryptor(map[string]any{"mode": "gcm", "iv": iv, "aad": aad, "tagsize": 32})
	assert.NoError(t, err)

	// encrypt once
	secret, err := enc.Encrypt([]byte(pt))
	assert.NoError(t, err)
	// decrypt once
	revert, err := decryptWithProps("gcm", key, secret, ivb, addb)
	assert.NoError(t, err)
	assert.Equal(t, pt, string(revert))
	// encrypt again
	secret, err = enc.Encrypt([]byte(pt))
	assert.NoError(t, err)
	// decrypt again
	revert, err = decryptWithProps("gcm", key, secret, ivb, addb)
	assert.NoError(t, err)
	assert.Equal(t, pt, string(revert))
}

func TestStreamWriter(t *testing.T) {
	key := []byte("0123456789abcdef01234567")
	// plaintext
	pt := "Using the Input type selection, choose the type of input – a text string or a file. In case of the text string input, enter your input into the Input text textarea1,2. Otherwise, use the \"Browse\" button to select the input file to upload. Then select the cryptographic function you want to use in the Function field. Depending on the selected function the Initialization vector (IV) field is shown or hidden. Initialization vector is always a sequence of bytes, each byte has to be represented in hexadecimal form."
	if conf.Config == nil {
		conf.Config = &conf.KuiperConf{}
	}
	conf.Config.AesKey = key
	output := new(bytes.Buffer)
	writer, err := GetEncryptWriter(output, map[string]any{"mode": "cfb"})
	assert.NoError(t, err)
	_, err = writer.Write([]byte(pt))
	assert.NoError(t, err)
	err = writer.(io.Closer).Close()
	assert.NoError(t, err)
	secret := output.Bytes()

	// Read the appended iv
	niv := secret[:aes.BlockSize]
	secret = secret[aes.BlockSize:]
	// Decrypt
	dstream, err := NewAESStreamDecrypter(key, niv)
	assert.NoError(t, err)
	revert := make([]byte, len(secret))
	dstream.XORKeyStream(revert, secret)
	assert.Equal(t, pt, string(revert))
}

func NewAESStreamDecrypter(key, iv []byte) (cipher.Stream, error) {
	// Create a new AES cipher block using the key
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("Error creating AES cipher block: %v", err)
	}
	return cipher.NewCFBDecrypter(block, iv), nil //nolint:staticcheck
}

func decrypt(mode string, key []byte, secret []byte) ([]byte, error) {
	switch mode {
	case "cfb":
		niv := secret[:aes.BlockSize]
		secret = secret[aes.BlockSize:]
		dstream, err := NewAESStreamDecrypter(key, niv)
		if err != nil {
			return nil, err
		}
		revert := make([]byte, len(secret))
		dstream.XORKeyStream(revert, secret)
		return revert, nil
	case "gcm":
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}

		nonceSize := gcm.NonceSize()
		if len(secret) < nonceSize {
			return nil, fmt.Errorf("ciphertext too short")
		}

		nonce, ciphertext := secret[:nonceSize], secret[nonceSize:]
		plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
		if err != nil {
			return nil, err
		}
		return plaintext, nil
	default:
		return nil, fmt.Errorf("Unknown mode: %s", mode)
	}
}

func decryptWithProps(mode string, key []byte, secret []byte, iv []byte, aad []byte) ([]byte, error) {
	switch mode {
	case "cfb":
		dstream, err := NewAESStreamDecrypter(key, iv)
		if err != nil {
			return nil, err
		}
		revert := make([]byte, len(secret))
		dstream.XORKeyStream(revert, secret)
		return revert, nil
	case "gcm":
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}

		nonceSize := gcm.NonceSize()
		if len(secret) < nonceSize {
			return nil, fmt.Errorf("ciphertext too short")
		}
		tag := secret[:16]
		secret = secret[32:]
		plaintext, err := gcm.Open(nil, iv, append(secret, tag...), aad)
		if err != nil {
			return nil, err
		}
		return plaintext, nil
	default:
		return nil, fmt.Errorf("Unknown mode: %s", mode)
	}
}

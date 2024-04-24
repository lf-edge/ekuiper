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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessage(t *testing.T) {
	key := []byte("0123456789abcdef01234567")
	iv := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
	// plaintext
	pt := "Using the Input type selection, choose the type of input – a text string or a file. In case of the text string input, enter your input into the Input text textarea1,2. Otherwise, use the \"Browse\" button to select the input file to upload. Then select the cryptographic function you want to use in the Function field. Depending on the selected function the Initialization vector (IV) field is shown or hidden. Initialization vector is always a sequence of bytes, each byte has to be represented in hexadecimal form."

	stream, err := NewStreamEncrypter(key, iv)
	assert.NoError(t, err)
	secret := stream.Encrypt([]byte(pt))

	niv := secret[:aes.BlockSize]
	assert.Equal(t, iv, niv)
	secret = secret[aes.BlockSize:]
	dstream, err := NewAESStreamDecrypter(key, iv)
	assert.NoError(t, err)
	revert := make([]byte, len(secret))
	dstream.XORKeyStream(revert, secret)
	assert.Equal(t, pt, string(revert))
}

func TestStreamWriter(t *testing.T) {
	key := []byte("0123456789abcdef01234567")
	iv := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
	// plaintext
	pt := "Using the Input type selection, choose the type of input – a text string or a file. In case of the text string input, enter your input into the Input text textarea1,2. Otherwise, use the \"Browse\" button to select the input file to upload. Then select the cryptographic function you want to use in the Function field. Depending on the selected function the Initialization vector (IV) field is shown or hidden. Initialization vector is always a sequence of bytes, each byte has to be represented in hexadecimal form."

	output := new(bytes.Buffer)
	writer, err := NewStreamWriter(key, iv, output)
	assert.NoError(t, err)
	_, err = writer.Write([]byte(pt))
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)
	secret := output.Bytes()

	// Read the appended iv
	niv := secret[:aes.BlockSize]
	assert.Equal(t, iv, niv)
	secret = secret[aes.BlockSize:]
	// Decrypt
	dstream, err := NewAESStreamDecrypter(key, iv)
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
	return cipher.NewCFBDecrypter(block, iv), nil
}

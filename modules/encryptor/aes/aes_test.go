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
	"encoding/base64"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
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
		conf.Config = &model.KuiperConf{}
	}
	conf.Config.AesKey = key
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, err := GetEncryptor(key, map[string]any{"mode": tt.mode})
			assert.NoError(t, err)
			dec, ok := enc.(message.Decryptor)
			require.True(t, ok)

			// encrypt once
			secret, err := enc.Encrypt([]byte(pt))
			assert.NoError(t, err)
			// decrypt once
			revert, err := dec.Decrypt(secret)
			assert.NoError(t, err)
			assert.Equal(t, pt, string(revert))
			// encrypt again
			secret, err = enc.Encrypt([]byte(pt))
			assert.NoError(t, err)
			// decrypt again
			revert, err = dec.Decrypt(secret)
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
		conf.Config = &model.KuiperConf{}
	}
	conf.Config.AesKey = key
	enc, err := GetEncryptor(key, map[string]any{"mode": "gcm", "iv": iv, "aad": aad, "tagsize": 32})
	assert.NoError(t, err)
	dec, ok := enc.(message.Decryptor)
	require.True(t, ok)

	// encrypt once
	secret, err := enc.Encrypt([]byte(pt))
	assert.NoError(t, err)
	// decrypt once
	revert, err := dec.Decrypt(secret)
	assert.NoError(t, err)
	assert.Equal(t, pt, string(revert))
	// encrypt again
	secret, err = enc.Encrypt([]byte(pt))
	assert.NoError(t, err)
	// decrypt again
	revert, err = dec.Decrypt(secret)
	assert.NoError(t, err)
	assert.Equal(t, pt, string(revert))
}

func TestStreamWriter(t *testing.T) {
	key := []byte("0123456789abcdef01234567")
	// plaintext
	pt := "Using the Input type selection, choose the type of input – a text string or a file. In case of the text string input, enter your input into the Input text textarea1,2. Otherwise, use the \"Browse\" button to select the input file to upload. Then select the cryptographic function you want to use in the Function field. Depending on the selected function the Initialization vector (IV) field is shown or hidden. Initialization vector is always a sequence of bytes, each byte has to be represented in hexadecimal form."
	output := new(bytes.Buffer)
	writer, err := GetEncryptWriter(output, key, map[string]any{"mode": "cfb"})
	assert.NoError(t, err)
	_, err = writer.Write([]byte(pt))
	assert.NoError(t, err)
	err = writer.(io.Closer).Close()
	assert.NoError(t, err)
	secret := output.Bytes()

	// Decrypt
	dec, err := GetDecryptor(key, map[string]any{"mode": "cfb"})
	assert.NoError(t, err)
	revert, err := dec.Decrypt(secret)
	assert.NoError(t, err)
	assert.Equal(t, pt, string(revert))
}

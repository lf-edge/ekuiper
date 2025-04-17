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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestGetEncryptor(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	_, err := GetEncryptor("aes", map[string]any{"mode": "gcm"}, &model.KuiperConf{AesKey: key})
	assert.NoError(t, err)
	_, err = GetEncryptor("unknown", map[string]any{"mode": "gcm"}, &model.KuiperConf{AesKey: key})
	assert.Error(t, err)
	_, err = GetEncryptor("aes", map[string]any{"mode": "cfb"}, &model.KuiperConf{AesKey: key})
	assert.NoError(t, err)
	_, err = GetEncryptor("aes", map[string]any{"mode": "abc"}, &model.KuiperConf{AesKey: key})
	assert.Error(t, err)
}

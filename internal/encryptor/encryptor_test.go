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

package encryptor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func TestGetEncryptor(t *testing.T) {
	conf.InitConf()
	_, err := GetEncryptor("aes", map[string]any{"mode": "gcm"})
	assert.NoError(t, err)
	_, err = GetEncryptor("unknown", map[string]any{"mode": "gcm"})
	assert.Error(t, err)
	_, err = GetEncryptor("aes", map[string]any{"mode": "cfb"})
	assert.NoError(t, err)
	_, err = GetEncryptor("aes", map[string]any{"mode": "abc"})
	assert.Error(t, err)
}

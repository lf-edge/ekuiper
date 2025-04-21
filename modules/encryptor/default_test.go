// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestDefault(t *testing.T) {
	conf := &model.EncryptionConf{
		Algorithm: "aes",
		Properties: map[string]any{
			"mode": "cfb",
		},
	}
	InitConf(conf, []byte("0123456789abcdef0123456789abcdef"))
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			dd, err := GetDefaultDecryptor()
			require.NoError(t, err)
			require.Equal(t, ddec, dd)
			wg.Done()
		}()
	}
	wg.Wait()
}

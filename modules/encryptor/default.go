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
	"errors"
	"sync"

	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

var (
	dec     message.Decryptor
	err     error
	encConf *model.EncryptionConf
	key     []byte
	once    sync.Once
)

// InitConf run in server start up
func InitConf(enc *model.EncryptionConf, k []byte) {
	encConf = enc
	key = k
}

// GetDefaultDecryptor returns the singleton of the decryptor
func GetDefaultDecryptor() (message.Decryptor, error) {
	once.Do(func() {
		if encConf != nil {
			dec, err = GetDecryptor(encConf.Algorithm, key, encConf.Properties)
		} else {
			err = errors.New("default encryption is not configured")
		}
	})
	return dec, err
}

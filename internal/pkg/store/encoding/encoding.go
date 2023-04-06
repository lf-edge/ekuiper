// Copyright 2021 EMQ Technologies Co., Ltd.
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

package encoding

import (
	"bytes"
	"encoding/gob"
	"time"
)

func Encode(value interface{}) ([]byte, error) {
	var buff bytes.Buffer
	gob.Register(time.Time{})
	gob.Register(value)
	enc := gob.NewEncoder(&buff)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

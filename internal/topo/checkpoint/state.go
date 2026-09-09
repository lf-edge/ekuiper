// Copyright 2026 EMQ Technologies Co., Ltd.
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

package checkpoint

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// FrozenStateStore accepts immutable, gob-encoded operator state. The bytes
// must not be changed after the call returns.
type FrozenStateStore interface {
	SaveFrozenState(checkpointID int64, opID string, state []byte) error
}

// FrozenStateDiscarder removes an incomplete checkpoint and rejects late task
// snapshots for that ID and every older checkpoint.
type FrozenStateDiscarder interface {
	DiscardFrozenState(checkpointID int64)
}

// EncodeState freezes a complete operator state graph.
func EncodeState(state map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return nil, fmt.Errorf("encode checkpoint state: %w", err)
	}
	return buf.Bytes(), nil
}

// DecodeState restores a complete operator state graph.
func DecodeState(state []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := gob.NewDecoder(bytes.NewReader(state)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode checkpoint state: %w", err)
	}
	return result, nil
}

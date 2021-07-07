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

package state

import (
	"sync"
)

type MemoryStore sync.Map //The current root store of a rule

func newMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) SaveState(_ int64, _ string, _ map[string]interface{}) error {
	//do nothing
	return nil
}

func (s *MemoryStore) SaveCheckpoint(_ int64) error {
	//do nothing
	return nil
}

func (s *MemoryStore) GetOpState(_ string) (*sync.Map, error) {
	return &sync.Map{}, nil
}

func (s *MemoryStore) Clean() error {
	return nil
}

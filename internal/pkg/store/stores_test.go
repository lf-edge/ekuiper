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

package store

import (
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/memory"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

func TestDropKVRemovesOnlyKVCacheEntry(t *testing.T) {
	const table = "shared-name"
	s := &stores{
		kv: map[string]kv.KeyValue{table: memory.NewMemoryKV()},
		ts: map[string]kv.Tskv{table: nil},
	}

	s.DropKV(table)
	if _, ok := s.kv[table]; ok {
		t.Fatal("dropped KV remained cached")
	}
	if _, ok := s.ts[table]; !ok {
		t.Fatal("dropping KV removed the unrelated TS cache entry")
	}
}

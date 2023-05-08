// Copyright 2022 EMQ Technologies Co., Ltd.
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

package binder

import (
	"reflect"
	"sort"
	"testing"

	"github.com/lf-edge/ekuiper/internal/binder/mock"
)

func TestEntriesSort(t *testing.T) {
	m := mock.NewMockFactory()
	e := FactoryEntry{
		Name:    "mock",
		Factory: m,
		Weight:  10,
	}
	e2 := FactoryEntry{
		Name:    "mock2",
		Factory: m,
		Weight:  5,
	}
	e3 := FactoryEntry{
		Name:    "mock3",
		Factory: m,
		Weight:  8,
	}
	entries := Entries{e, e2, e3}
	sort.Sort(entries)

	expect := Entries{e, e3, e2}

	if reflect.DeepEqual(entries, expect) == false {
		t.Errorf("sort error, expect: %v, actual: %v", expect, entries)
	}
}

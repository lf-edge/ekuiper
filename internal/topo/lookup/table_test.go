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

package lookup

import (
	"github.com/lf-edge/ekuiper/pkg/ast"
	"sync"
	"testing"
)

func TestTable(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	err := CreateInstance("test1", "memory", &ast.Options{
		DATASOURCE: "test",
		TYPE:       "memory",
		KIND:       "lookup",
		KEY:        "id",
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = CreateInstance("test2", "memory", &ast.Options{
		DATASOURCE: "test2",
		TYPE:       "memory",
		KIND:       "lookup",
		KEY:        "id",
	})
	if err != nil {
		t.Error(err)
		return
	}
	go func() {
		for i := 0; i < 3; i++ {
			_, err := Attach("test1")
			if err != nil {
				t.Error(err)
			}
			_, err = Attach("test2")
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 3; i++ {
			_, err := Attach("test2")
			if err != nil {
				t.Error(err)
			}
			_, err = Attach("test1")
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()
	wg.Wait()
	if len(instances) != 2 {
		t.Errorf("expect 2 instances, but got %d", len(instances))
		return
	}
	for _, ins := range instances {
		if ins.count != 6 {
			t.Errorf("expect 6 count, but got %d", ins.count)
			return
		}
	}
	err = DropInstance("test1")
	if err == nil {
		t.Error("should have error to drop instance")
		return
	}
	for i := 0; i < 6; i++ {
		Detach("test1")
	}
	err = DropInstance("test1")
	if err != nil {
		t.Error(err)
		return
	}
	if len(instances) != 1 {
		t.Errorf("expect 2 instances, but got %d", len(instances))
		return
	}
}

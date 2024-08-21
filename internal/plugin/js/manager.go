// Copyright 2024 EMQ Technologies Co., Ltd.
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

package js

import (
	"encoding/json"
	"fmt"

	"github.com/dop251/goja"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

var (
	manager *Manager
	_       binder.FuncFactory = manager
)

func GetManager() *Manager {
	return manager
}

type Manager struct {
	db             kv.KeyValue
	importStatusDb kv.KeyValue
}

type Script struct {
	Id     string `json:"id"`
	Desc   string `json:"description"`
	Script string `json:"script"`
	IsAgg  bool   `json:"isAgg"`
}

// InitManager initialize the manager, only called once by the server
func InitManager() error {
	db, err := store.GetKV("script")
	if err != nil {
		return fmt.Errorf("can not initialize store for the JavaScript function manager at path 'script': %v", err)
	}
	importStatusDb, err := store.GetKV("scriptInstallStatus")
	if err != nil {
		return fmt.Errorf("can not initialize store for the JavaScript function manager at path 'scriptInstallStatus': %v", err)
	}

	manager = &Manager{
		db:             db,
		importStatusDb: importStatusDb,
	}
	return nil
}

func (m *Manager) UpsertByJson(k string, v string) error {
	s := &Script{Id: k}
	err := json.Unmarshal([]byte(v), s)
	if err != nil {
		return fmt.Errorf("fail to unmarshal the script %s: %v", k, err)
	}
	if s.Id != k {
		return fmt.Errorf("the script id %s does not match the key %s", s.Id, k)
	}
	return m.Update(s)
}

func (m *Manager) Create(script *Script) error {
	err := validate(script)
	if err != nil {
		return err
	}
	return m.db.Setnx(script.Id, script)
}

func validate(script *Script) error {
	vm := goja.New()
	_, err := vm.RunString(script.Script)
	if err != nil {
		return fmt.Errorf("failed to interprete script: %v", err)
	}
	_, ok := goja.AssertFunction(vm.Get(script.Id))
	if !ok {
		return fmt.Errorf("cannot find function \"%s\" in script", script.Id)
	}
	return nil
}

func (m *Manager) GetScript(id string) (*Script, error) {
	result := &Script{}
	ok, err := m.db.Get(id, result)
	if !ok && err == nil {
		return nil, fmt.Errorf("not found")
	}
	return result, err
}

func (m *Manager) List() ([]string, error) {
	return m.db.Keys()
}

func (m *Manager) Update(script *Script) error {
	err := validate(script)
	if err != nil {
		return err
	}
	return m.db.Set(script.Id, script)
}

func (m *Manager) Delete(id string) error {
	return m.db.Delete(id)
}

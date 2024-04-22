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
	"context"
	"encoding/json"

	"github.com/lf-edge/ekuiper/internal/conf"
)

// Exporter is used to export and import the JavaScript functions
// The functions are stored in the key-value store only. So import and export are just to read and write the key-value store

// Import the JavaScript functions from the map. This is usually called after reset to override all settings
func (m *Manager) Import(ctx context.Context, scripts map[string]string) map[string]string {
	errMap := map[string]string{}
	_ = m.importStatusDb.Clean()
	for k, v := range scripts {
		select {
		case <-ctx.Done():
			return errMap
		default:
		}
		err := m.UpsertByJson(k, v)
		if err != nil {
			_ = m.importStatusDb.Set(k, err.Error())
			errMap[k] = err.Error()
		}
	}
	return errMap
}

func (m *Manager) PartialImport(ctx context.Context, scripts map[string]string) map[string]string {
	errMap := map[string]string{}
	for k, v := range scripts {
		select {
		case <-ctx.Done():
			return errMap
		default:
		}
		err := m.UpsertByJson(k, v)
		if err != nil {
			errMap[k] = err.Error()
		}
	}
	return errMap
}

func (m *Manager) Export() map[string]string {
	all, err := m.db.Keys()
	if err != nil {
		conf.Log.Errorf("Fail to export the JavaScript function manager: %v", err)
		return nil
	}
	result := make(map[string]string, len(all))
	for _, k := range all {
		s, err := m.GetScript(k)
		if err != nil {
			conf.Log.Errorf("Fail to export the JavaScript function %s: %v", k, err)
			continue
		}
		sj, err := json.Marshal(s)
		if err != nil {
			conf.Log.Errorf("Fail to marshal the JavaScript function %s: %v", k, err)
			continue
		}
		result[k] = string(sj)
	}
	return result
}

func (m *Manager) Status() map[string]string {
	all, err := m.importStatusDb.All()
	if err != nil {
		return nil
	}
	return all
}

func (m *Manager) Reset() {
	err := m.db.Clean()
	if err != nil {
		conf.Log.Errorf("Fail to reset the JavaScript function manager: %v", err)
	}
}

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

package xsql

import "unsafe"

func (t *Tuple) CalculateMemUsage() {
	var mem int64
	for key, value := range t.Message {
		mem += int64(unsafe.Sizeof(key))
		mem += getMemUsage(value)
	}
	t.MemUsage = mem
}

func (t *Tuple) GetMemUsage() int64 {
	return t.MemUsage
}

func getMemUsage(v interface{}) int64 {
	var mem int64
	switch vv := v.(type) {
	case int:
		mem += int64(unsafe.Sizeof(vv))
	case float32:
		mem += int64(unsafe.Sizeof(vv))
	case int64:
		mem += int64(unsafe.Sizeof(vv))
	case float64:
		mem += int64(unsafe.Sizeof(vv))
	case string:
		mem += int64(unsafe.Sizeof(vv))
	case bool:
		mem += int64(unsafe.Sizeof(vv))
	case map[string]interface{}:
		for key, value := range vv {
			mem += int64(unsafe.Sizeof(key))
			mem += getMemUsage(value)
		}
	case []interface{}:
		for _, value := range vv {
			mem += getMemUsage(value)
		}
	}
	return mem
}

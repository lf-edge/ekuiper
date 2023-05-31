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

package main

import (
	"fmt"
	"path/filepath"
	"sync"

	tflite "github.com/mattn/go-tflite" //nolint:typecheck

	"github.com/lf-edge/ekuiper/internal/conf"
)

var ipManager *interpreterManager

func init() {
	path, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	ipManager = &interpreterManager{
		registry: make(map[string]*tflite.Interpreter),
		path:     filepath.Join(path, "uploads"),
	}
}

type interpreterManager struct {
	sync.Mutex
	registry map[string]*tflite.Interpreter
	path     string
}

func (m *interpreterManager) GetOrCreate(name string) (*tflite.Interpreter, error) {
	m.Lock()
	defer m.Unlock()
	ip, ok := m.registry[name]
	if !ok {
		mf := filepath.Join(m.path, name+".tflite")
		model := tflite.NewModelFromFile(mf)
		if model == nil {
			return nil, fmt.Errorf("fail to load model: %s", mf)
		}
		defer model.Delete()
		options := tflite.NewInterpreterOptions()
		options.SetNumThread(4)
		options.SetErrorReporter(func(msg string, user_data interface{}) {
			fmt.Println(msg)
		}, nil)
		defer options.Delete()
		ip = tflite.NewInterpreter(model, options)
		status := ip.AllocateTensors()
		if status != tflite.OK {
			ip.Delete()
			return nil, fmt.Errorf("allocate failed: %v", status)
		}
		m.registry[name] = ip
	}
	return ip, nil
}

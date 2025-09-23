// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

//go:build tflite

package tflite

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/mattn/go-tflite"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
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
	log := conf.Log
	m.Lock()
	defer m.Unlock()
	ip, ok := m.registry[name]
	if !ok {
		mf := filepath.Join(m.path, name+".tflite")
		model := tflite.NewModelFromFile(mf)
		if model == nil {
			log.Errorf("fail to load model: %s", mf)
			return nil, fmt.Errorf("fail to load model: %s", mf)
		}
		log.Infof("success load model: %s", mf)
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
			log.Errorf("allocate tensors failed for: %s", mf)
			ip.Delete()
			return nil, fmt.Errorf("allocate failed: %v", status)
		}
		log.Infof("success allocate tensors for: %s", mf)
		m.registry[name] = ip
	}
	return ip, nil
}

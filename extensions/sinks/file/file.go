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

package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"os"
	"sync"
	"time"
)

type fileSink struct {
	interval int
	path     string

	results [][]byte
	file    *os.File
	mux     sync.Mutex
	cancel  context.CancelFunc
}

func (m *fileSink) Configure(props map[string]interface{}) error {
	m.interval = 1000
	m.path = "cache"
	if i, ok := props["interval"]; ok {
		if i, ok := i.(float64); ok {
			m.interval = int(i)
		}
	}
	if i, ok := props["path"]; ok {
		if i, ok := i.(string); ok {
			m.path = i
		}
	}
	return nil
}

func (m *fileSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Debug("Opening file sink")
	m.results = make([][]byte, 0)
	var f *os.File
	var err error
	if _, err := os.Stat(m.path); os.IsNotExist(err) {
		_, err = os.Create(m.path)
	}
	f, err = os.OpenFile(m.path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return fmt.Errorf("fail to open file sink for %v", err)
	}
	m.file = f
	t := time.NewTicker(time.Duration(m.interval) * time.Millisecond)
	exeCtx, cancel := ctx.WithCancel()
	m.cancel = cancel
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				m.save(logger)
			case <-exeCtx.Done():
				logger.Info("file sink done")
				return
			}
		}
	}()
	return nil
}

func (m *fileSink) save(logger api.Logger) {
	if len(m.results) == 0 {
		return
	}
	logger.Debugf("file sink is saving to file %s", m.path)
	var strings []string
	m.mux.Lock()
	for _, b := range m.results {
		strings = append(strings, string(b)+"\n")
	}
	m.results = make([][]byte, 0)
	m.mux.Unlock()
	w := bufio.NewWriter(m.file)
	for _, s := range strings {
		_, err := m.file.WriteString(s)
		if err != nil {
			logger.Errorf("file sink fails to write out result '%s' with error %s.", s, err)
		}
	}
	w.Flush()
	logger.Debugf("file sink has saved to file %s", m.path)
}

func (m *fileSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if v, ok := item.([]byte); ok {
		logger.Debugf("file sink receive %s", item)
		m.mux.Lock()
		m.results = append(m.results, v)
		m.mux.Unlock()
	} else {
		logger.Debug("file sink receive non byte data")
	}
	return nil
}

func (m *fileSink) Close(ctx api.StreamContext) error {
	if m.cancel != nil {
		m.cancel()
	}
	if m.file != nil {
		m.save(ctx.GetLogger())
		return m.file.Close()
	}
	return nil
}

func File() api.Sink {
	return &fileSink{}
}

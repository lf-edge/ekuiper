// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package io

import (
	"github.com/lf-edge/ekuiper/internal/io/file"
	"github.com/lf-edge/ekuiper/internal/io/http"
	"github.com/lf-edge/ekuiper/internal/io/memory"
	"github.com/lf-edge/ekuiper/internal/io/mqtt"
	"github.com/lf-edge/ekuiper/internal/io/neuron"
	"github.com/lf-edge/ekuiper/internal/io/sink"
	plugin2 "github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type NewSourceFunc func() api.Source
type NewLookupSourceFunc func() api.LookupSource
type NewSinkFunc func() api.Sink

var (
	sources = map[string]NewSourceFunc{
		"mqtt":     func() api.Source { return &mqtt.MQTTSource{} },
		"httppull": func() api.Source { return &http.PullSource{} },
		"httppush": func() api.Source { return &http.PushSource{} },
		"file":     func() api.Source { return &file.FileSource{} },
		"memory":   func() api.Source { return memory.GetSource() },
		"neuron":   func() api.Source { return neuron.GetSource() },
	}
	sinks = map[string]NewSinkFunc{
		"log":         sink.NewLogSink,
		"logToMemory": sink.NewLogSinkToMemory,
		"mqtt":        func() api.Sink { return &mqtt.MQTTSink{} },
		"rest":        func() api.Sink { return &http.RestSink{} },
		"nop":         func() api.Sink { return &sink.NopSink{} },
		"memory":      func() api.Sink { return memory.GetSink() },
		"neuron":      func() api.Sink { return neuron.GetSink() },
		"file":        func() api.Sink { return file.File() },
	}
	lookupSources = map[string]NewLookupSourceFunc{
		"memory": func() api.LookupSource { return memory.GetLookupSource() },
	}
)

type Manager struct{}

func (m *Manager) Source(name string) (api.Source, error) {
	if s, ok := sources[name]; ok {
		return s(), nil
	}
	return nil, nil
}

func (m *Manager) GetSourcePlugin(_ string) (plugin2.EXTENSION_TYPE, string, string) {
	return plugin2.INTERNAL, "", ""
}

func (m *Manager) LookupSource(name string) (api.LookupSource, error) {
	if s, ok := lookupSources[name]; ok {
		return s(), nil
	}
	return nil, nil
}

func (m *Manager) Sink(name string) (api.Sink, error) {
	if s, ok := sinks[name]; ok {
		return s(), nil
	}
	return nil, nil
}

func (m *Manager) GetSinkPlugin(_ string) (plugin2.EXTENSION_TYPE, string, string) {
	return plugin2.INTERNAL, "", ""
}

var m = &Manager{}

func GetManager() *Manager {
	return m
}

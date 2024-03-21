// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/io/simulator"
	"github.com/lf-edge/ekuiper/internal/io/sink"
	"github.com/lf-edge/ekuiper/internal/io/websocket"
	plugin2 "github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/modules"
)

func init() {
	modules.RegisterSource("mqtt", func() api.Source { return &mqtt.SourceConnector{} })
	modules.RegisterSource("httppull", func() api.Source { return &http.PullSource{} })
	modules.RegisterSource("httppush", func() api.Source { return &http.PushSource{} })
	modules.RegisterSource("file", func() api.Source { return &file.FileSource{} })
	modules.RegisterSource("memory", func() api.Source { return memory.GetSource() })
	modules.RegisterSource("neuron", func() api.Source { return neuron.GetSource() })
	modules.RegisterSource("websocket", func() api.Source { return &websocket.WebsocketSource{} })
	modules.RegisterSource("simulator", func() api.Source { return &simulator.Source{} })

	modules.RegisterSink("log", sink.NewLogSink)
	modules.RegisterSink("logToMemory", sink.NewLogSinkToMemory)
	modules.RegisterSink("mqtt", func() api.Sink { return &mqtt.MQTTSink{} })
	modules.RegisterSink("rest", func() api.Sink { return &http.RestSink{} })
	modules.RegisterSink("nop", func() api.Sink { return &sink.NopSink{} })
	modules.RegisterSink("memory", func() api.Sink { return memory.GetSink() })
	modules.RegisterSink("neuron", func() api.Sink { return neuron.GetSink() })
	modules.RegisterSink("file", func() api.Sink { return file.File() })
	modules.RegisterSink("websocket", func() api.Sink { return &websocket.WebSocketSink{} })

	modules.RegisterLookupSource("memory", func() api.LookupSource { return memory.GetLookupSource() })
	modules.RegisterLookupSource("httppull", func() api.LookupSource { return http.GetLookUpSource() })
}

type Manager struct{}

func (m *Manager) Source(name string) (api.Source, error) {
	if s, ok := modules.Sources[name]; ok {
		return s(), nil
	}
	return nil, nil
}

func (m *Manager) SourcePluginInfo(name string) (plugin2.EXTENSION_TYPE, string, string) {
	if _, ok := modules.Sources[name]; ok {
		return plugin2.INTERNAL, "", ""
	} else {
		return plugin2.NONE_EXTENSION, "", ""
	}
}

func (m *Manager) LookupSource(name string) (api.LookupSource, error) {
	if s, ok := modules.LookupSources[name]; ok {
		return s(), nil
	}
	return nil, nil
}

func (m *Manager) Sink(name string) (api.Sink, error) {
	if s, ok := modules.Sinks[name]; ok {
		return s(), nil
	}
	return nil, nil
}

func (m *Manager) SinkPluginInfo(name string) (plugin2.EXTENSION_TYPE, string, string) {
	if _, ok := modules.Sinks[name]; ok {
		return plugin2.INTERNAL, "", ""
	} else {
		return plugin2.NONE_EXTENSION, "", ""
	}
}

var m = &Manager{}

func GetManager() *Manager {
	return m
}

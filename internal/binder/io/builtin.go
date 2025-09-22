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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/io/file"
	"github.com/lf-edge/ekuiper/v2/internal/io/http"
	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory"
	"github.com/lf-edge/ekuiper/v2/internal/io/mqtt"
	"github.com/lf-edge/ekuiper/v2/internal/io/neuron"
	"github.com/lf-edge/ekuiper/v2/internal/io/nexmark"
	"github.com/lf-edge/ekuiper/v2/internal/io/simulator"
	"github.com/lf-edge/ekuiper/v2/internal/io/sink"
	"github.com/lf-edge/ekuiper/v2/internal/io/websocket"
	plugin2 "github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/nng"
)

func init() {
	modules.RegisterSource("mqtt", mqtt.GetSource)
	modules.RegisterSource("httppull", func() api.Source { return &http.HttpPullSource{} })
	modules.RegisterSource("httppush", func() api.Source { return &http.HttpPushSource{} })
	modules.RegisterSource("file", file.GetSource)
	modules.RegisterSource("memory", func() api.Source { return memory.GetSource() })
	modules.RegisterSource("neuron", neuron.GetSource)
	modules.RegisterSource("websocket", func() api.Source { return websocket.GetSource() })
	modules.RegisterSource("simulator", func() api.Source { return simulator.GetSource() })
	modules.RegisterSource("nexmark", func() api.Source { return nexmark.GetSource() })

	modules.RegisterSink("log", sink.NewLogSink)
	modules.RegisterSink("logToMemory", sink.NewLogSinkToMemory)
	modules.RegisterSink("mqtt", mqtt.GetSink)
	modules.RegisterSink("rest", func() api.Sink { return http.GetSink() })
	modules.RegisterSink("nop", func() api.Sink { return &sink.NopSink{} })
	modules.RegisterSink("memory", func() api.Sink { return memory.GetSink() })
	modules.RegisterSink("neuron", neuron.GetSink)
	modules.RegisterSink("file", file.GetSink)
	modules.RegisterSink("websocket", func() api.Sink { return websocket.GetSink() })

	modules.RegisterLookupSource("memory", memory.GetLookupSource)
	modules.RegisterLookupSource("httppull", http.GetLookUpSource)
	modules.RegisterLookupSource("simulator", func() api.Source { return &simulator.SimulatorLookupSource{} })

	modules.RegisterConnection("mqtt", mqtt.CreateConnection)
	modules.RegisterConnection("nng", nng.CreateConnection)
	modules.RegisterConnection("httppush", httpserver.CreateConnection)
	modules.RegisterConnection("websocket", httpserver.CreateWebsocketConnection)
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

func (m *Manager) LookupSource(name string) (api.Source, error) {
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

var (
	m                      = &Manager{}
	_ binder.SourceFactory = m
	_ binder.SinkFactory   = m
)

func GetManager() *Manager {
	return m
}

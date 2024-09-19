// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracer

import (
	"encoding/json"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

type TracerConfig struct {
	EnableRemoteCollector bool   `json:"enableRemoteCollector"`
	ServiceName           string `json:"serviceName"`
	RemoteEndpoint        string `json:"remoteEndpoint"`
}

func TracerConfigFromConf() *TracerConfig {
	return &TracerConfig{
		EnableRemoteCollector: conf.Config.OpenTelemetry.EnableRemoteCollector,
		ServiceName:           conf.Config.OpenTelemetry.ServiceName,
		RemoteEndpoint:        conf.Config.OpenTelemetry.RemoteEndpoint,
	}
}

type LocalSpan struct {
	Name         string                 `json:"name"`
	TraceID      string                 `json:"traceID"`
	SpanID       string                 `json:"spanID"`
	ParentSpanID string                 `json:"parentSpanID,omitempty"`
	Attribute    map[string]interface{} `json:"attribute,omitempty"`
	Links        []LocalLink            `json:"links,omitempty"`
	StartTime    time.Time              `json:"startTime"`
	EndTime      time.Time              `json:"endTime"`
	RuleID       string                 `json:"ruleID"`

	ChildSpan []*LocalSpan
}

type LocalLink struct {
	TraceID string `yaml:"traceID"`
}

func (span *LocalSpan) ToBytes() ([]byte, error) {
	return json.Marshal(span)
}

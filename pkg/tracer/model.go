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

package tracer

import (
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type LocalSpan struct {
	Name         string                 `yaml:"name"`
	TraceID      string                 `yaml:"traceID"`
	SpanID       string                 `yaml:"spanID"`
	ParentSpanID string                 `yaml:"parentSpanID,omitempty"`
	Attribute    map[string]interface{} `yaml:"attribute,omitempty"`
	Links        []LocalLink            `yaml:"links,omitempty"`
	StartTime    time.Time              `yaml:"startTime"`
	EndTime      time.Time              `yaml:"endTime"`
	ruleID       string                 `yaml:"ruleID"`

	ChildSpan []*LocalSpan
}

type LocalLink struct {
	TraceID string `yaml:"traceID"`
}

func FromReadonlySpan(readonly sdktrace.ReadOnlySpan) *LocalSpan {
	span := &LocalSpan{
		Name:         readonly.Name(),
		TraceID:      readonly.SpanContext().TraceID().String(),
		SpanID:       readonly.SpanContext().SpanID().String(),
		ParentSpanID: readonly.Parent().SpanID().String(),
		ChildSpan:    make([]*LocalSpan, 0),
		StartTime:    readonly.StartTime(),
		EndTime:      readonly.EndTime(),
	}
	if len(readonly.Attributes()) > 0 {
		span.Attribute = make(map[string]interface{})
		for _, attr := range readonly.Attributes() {
			if string(attr.Key) == "rule" {
				span.ruleID = attr.Value.AsString()
			}
			span.Attribute[string(attr.Key)] = attr.Value.AsInterface()
		}
	}
	if len(readonly.Links()) > 0 {
		span.Links = make([]LocalLink, 0)
		for _, link := range readonly.Links() {
			span.Links = append(span.Links, LocalLink{
				TraceID: link.SpanContext.TraceID().String(),
			})
		}
	}
	return span
}

// Copyright 2023 EMQ Technologies Co., Ltd.
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

package mqtt

import (
	"os"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func BenchmarkGetSimpleTuples(b *testing.B) {
	contextLogger := conf.Log.WithField("rule", "BenchmarkTuple_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	cv, _ := converter.GetOrCreateConverter(&ast.Options{FORMAT: "json"})
	ctx = context.WithValue(ctx, context.DecodeKey, cv)
	ms := &MQTTSource{}
	// Prepare a compressed payload
	originalPayload := []byte(`{"key": "value"}`)
	// Create a mock MQTT message with the compressed payload
	msg := MockMessage{
		payload: originalPayload,
		topic:   "test/topic",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getTuples(ctx, ms, msg)
	}
}

func BenchmarkGetComplexTuples(b *testing.B) {
	contextLogger := conf.Log.WithField("rule", "BenchmarkTuple_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	cv, _ := converter.GetOrCreateConverter(&ast.Options{FORMAT: "json"})
	ctx = context.WithValue(ctx, context.DecodeKey, cv)
	ms := &MQTTSource{}
	payload, err := os.ReadFile("./testdata/MDFD.json")
	if err != nil {
		b.Fatalf(err.Error())
	}
	msg := MockMessage{
		payload: payload,
		topic:   "test/topic",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getTuples(ctx, ms, msg)
	}
}

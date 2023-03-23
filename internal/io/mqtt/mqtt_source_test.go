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
	"bytes"
	"compress/zlib"
	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"reflect"
	"testing"
)

// TestGetTupleWithZlibCompressor is a unit test for getTuple with zlib compressor
func TestGetTupleWithZlibCompressor(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "TestTupleZlib_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	cv, _ := converter.GetOrCreateConverter(&ast.Options{FORMAT: "json"})
	ctx = context.WithValue(ctx, context.DecodeKey, cv)
	dc, _ := compressor.GetDecompressor("zlib")
	ms := &MQTTSource{
		decompressor: dc,
	}

	// Prepare a compressed payload
	originalPayload := []byte(`{"key": "value"}`)
	var buf bytes.Buffer
	zlibWriter := zlib.NewWriter(&buf)
	_, _ = zlibWriter.Write(originalPayload)
	_ = zlibWriter.Close()
	compressedPayload := buf.Bytes()

	// Create a mock MQTT message with the compressed payload
	msg := MockMessage{
		payload: compressedPayload,
		topic:   "test/topic",
	}
	// Call getTuple with the mock MQTT message
	result := getTuple(ctx, ms, msg)

	// Check if the result is a valid SourceTuple and has the correct content
	if st, ok := result.(api.SourceTuple); ok {
		if !reflect.DeepEqual(st.Message(), map[string]interface{}{"key": "value"}) {
			t.Errorf("Expected message to be %v, but got %v", map[string]interface{}{"key": "value"}, st.Message())
		}
		if !reflect.DeepEqual(st.Meta(), map[string]interface{}{"topic": "test/topic", "messageid": "1"}) {
			t.Errorf("Expected metadata to be %v, but got %v", map[string]interface{}{"topic": "test/topic", "messageid": "1"}, st.Meta())
		}
	} else {
		t.Errorf("Expected result to be a SourceTuple, but got %T", result)
	}
}

type MockMessage struct {
	payload []byte
	topic   string
}

func (mm MockMessage) Payload() []byte {
	return mm.payload
}

func (MockMessage) Duplicate() bool {
	panic("function not expected to be invoked")
}

func (MockMessage) Qos() byte {
	panic("function not expected to be invoked")
}

func (MockMessage) Retained() bool {
	panic("function not expected to be invoked")
}

func (mm MockMessage) Topic() string {
	return mm.topic
}

func (MockMessage) MessageID() uint16 {
	return 1
}

func (MockMessage) Ack() {
	panic("function not expected to be invoked")
}

// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package topo

import (
	"fmt"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSubtopoLC(t *testing.T) {
	ctx1 := mockContext.NewMockContext("rule1", "abc").WithRun(1)
	assert.Equal(t, 0, len(subTopoPool))
	subTopo, existed := GetOrCreateSubTopo(ctx1, "shared", false)
	assert.False(t, existed)
	// Test creation
	srcNode := &mockSrc{name: "shared"}
	opNode := &mockOp{name: "op1", ch: make(chan any)}
	subTopo.AddSrc(srcNode)
	subTopo.AddOperator([]node.Emitter{srcNode}, opNode)
	subTopo.StoreSchema("rule1", "shared", map[string]*ast.JsonStreamField{
		"field1": {Type: "string"},
	}, false)
	assert.Equal(t, 1, len(subTopoPool))
	assert.Equal(t, srcNode, subTopo.GetSource())
	assert.Equal(t, []node.OperatorNode{opNode}, subTopo.ops)
	assert.Equal(t, opNode, subTopo.tail)
	assert.Equal(t, 1, subTopo.OpsCount())
	// Test linkage
	assert.Equal(t, 1, len(srcNode.outputs))
	var tch chan<- any = opNode.ch
	assert.Equal(t, tch, srcNode.outputs[0])
	ptopo := &def.PrintableTopo{
		Sources: []string{"source_shared"},
		Edges: map[string][]any{
			"source_shared": {"op_shared_op1"},
		},
	}
	assert.Equal(t, ptopo, subTopo.topo)
	// Test run
	subTopo.Open(ctx1, make(chan error))
	assert.Equal(t, 1, len(subTopo.refRules))
	assert.Equal(t, 1, opNode.schemaCount)
	// Run another
	ctx2 := mockContext.NewMockContext("rule2", "abc").WithRun(2)
	subTopo2, existed := GetOrCreateSubTopo(ctx2, "shared", false)
	assert.True(t, existed)
	assert.Equal(t, subTopo, subTopo2)
	subTopo.StoreSchema("rule2", "shared", map[string]*ast.JsonStreamField{
		"field2": {Type: "string"},
	}, false)
	subTopo2.Open(ctx2, make(chan error))
	assert.Equal(t, 2, len(subTopo.refRules))
	assert.Equal(t, 2, opNode.schemaCount)
	// Metrics test
	metrics := []any{0, 0, 0, 0, 0, 0, 0, "", 0, 0, 0, 0, 0, 0, 0, 0, "", 0}
	assert.Equal(t, metrics, subTopo.GetMetrics())
	keys := []string{"source_shared_0_records_in_total", "source_shared_0_records_out_total", "source_shared_0_messages_processed_total", "source_shared_0_process_latency_us", "source_shared_0_buffer_length", "source_shared_0_last_invocation", "source_shared_0_exceptions_total", "source_shared_0_last_exception", "source_shared_0_last_exception_time", "op_shared_op1_0_records_in_total", "op_shared_op1_0_records_out_total", "op_shared_op1_0_messages_processed_total", "op_shared_op1_0_process_latency_us", "op_shared_op1_0_buffer_length", "op_shared_op1_0_last_invocation", "op_shared_op1_0_exceptions_total", "op_shared_op1_0_last_exception", "op_shared_op1_0_last_exception_time"}
	kk, vv := subTopo2.SubMetrics()
	assert.Equal(t, len(keys), len(metrics))
	assert.Equal(t, keys, kk)
	assert.Equal(t, metrics, vv)
	// Append to rule
	och := make(chan any)
	err := subTopo.AddOutput(och, "opp")
	assert.NoError(t, err)
	var ochOut chan<- any = och
	assert.Equal(t, 1, len(opNode.outputs))
	assert.Equal(t, ochOut, opNode.outputs[0])
	och2 := make(chan any)
	err = subTopo2.AddOutput(och2, "opp")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(opNode.outputs))
	var ochOut2 chan<- any = och2
	assert.Equal(t, ochOut2, opNode.outputs[1])
	// Checkpoint
	var (
		sources []checkpoint.StreamTask
		ops     []checkpoint.NonSourceTask
	)
	subTopo.EnableCheckpoint(&sources, &ops)
	assert.Equal(t, []checkpoint.StreamTask{srcNode}, sources)
	assert.Equal(t, []checkpoint.NonSourceTask{opNode}, ops)
	// Stop
	subTopo.Close(ctx1)
	assert.Equal(t, 1, len(subTopo.refRules))
	assert.Equal(t, 1, len(subTopoPool))
	subTopo2.Close(ctx2)
	assert.Equal(t, 0, len(subTopo.refRules))
	assert.Equal(t, 0, len(subTopoPool))
}

// Test when connection fails
func TestSubtopoRunError(t *testing.T) {
	ctx0 := mockContext.NewMockContext("rule0", "abc").WithRun(0)
	assert.Equal(t, 0, len(subTopoPool))
	subTopo, existed := GetOrCreateSubTopo(ctx0, "re", false)
	assert.False(t, existed)
	srcNode := &mockSrc{name: "src1", sentError: true}
	opNode := &mockOp{name: "op1", ch: make(chan any)}
	subTopo.AddSrc(srcNode)
	subTopo.AddOperator([]node.Emitter{srcNode}, opNode)
	// create another subtopo
	ctx1 := mockContext.NewMockContext("rule1", "abc").WithRun(1)
	subTopo2, existed := GetOrCreateSubTopo(ctx1, "re", false)
	assert.True(t, existed)
	assert.Equal(t, subTopo, subTopo2)
	assert.Equal(t, 1, len(subTopoPool))
	assert.Equal(t, InitState, subTopo.opened.Load())
	subTopo.Open(ctx0, make(chan<- error))
	// Test run secondly and thirdly, should fail
	errCh1 := make(chan error, 1)
	subTopo.Open(ctx1, errCh1)
	assert.Equal(t, 2, len(subTopo.refRules))
	errCh2 := make(chan error, 1)
	assert.Equal(t, OpenState, subTopo.opened.Load())
	ctx2 := mockContext.NewMockContext("rule2", "abc").WithRun(2)
	subTopo.Open(ctx2, errCh2)
	assert.Equal(t, 3, len(subTopo.refRules))
	select {
	case err := <-errCh1:
		assert.Equal(t, assert.AnError, err)
		subTopo.Close(ctx1)
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Should receive error")
	}
	select {
	case err := <-errCh2:
		assert.Equal(t, assert.AnError, err)
		subTopo2.Close(ctx2)
	case <-time.After(1 * time.Second):
		assert.Fail(t, "Should receive error")
	}
	assert.Equal(t, CloseState, subTopo.opened.Load())
	assert.Equal(t, 1, len(subTopo.refRules))
	subTopo.Close(ctx0)
	assert.Equal(t, 0, len(subTopoPool))
}

func TestSubtopoPrint(t *testing.T) {
	tt := &def.PrintableTopo{
		Sources: []string{"source_shared"},
		Edges: map[string][]any{
			"source_shared": {"op_shared_op1"},
		},
	}
	ctx0 := mockContext.NewMockContext("rule0", "abc")
	subTopo, _ := GetOrCreateSubTopo(ctx0, "shared", false)
	subTopo.topo = tt
	subTopo.tail = &mockOp{name: "op1", ch: make(chan any)}
	ptopo := &def.PrintableTopo{
		Sources: []string{"mqtt_src1"},
		Edges:   map[string][]any{},
	}
	subTopo.MergeSrc(ptopo)
	assert.Equal(t, &def.PrintableTopo{
		Sources: []string{"mqtt_src1", "source_shared"},
		Edges:   map[string][]any{"source_shared": {"op_shared_op1"}},
	}, ptopo)
	subTopo.LinkTopo(ptopo, "project")
	assert.Equal(t, &def.PrintableTopo{
		Sources: []string{"mqtt_src1", "source_shared"},
		Edges: map[string][]any{
			"op_shared_op1": {"op_project"},
			"source_shared": {"op_shared_op1"},
		},
	}, ptopo)
	RemoveSubTopo("shared")
}

// because subtopo create and open is not atomic
// test close in-between, which is supposed to close finally
func TestSubtopoConcurrency(t *testing.T) {
	// These are happened during planning syncly
	ctx := mockContext.NewMockContext("rule1", "abc").WithRun(1)
	assert.Equal(t, 0, len(subTopoPool))
	subTopo, existed := GetOrCreateSubTopo(ctx, "shared", false)
	assert.False(t, existed)
	srcNode := &mockSrc{name: "shared"}
	opNode := &mockOp{name: "op1", ch: make(chan any)}
	subTopo.AddSrc(srcNode)
	subTopo.AddOperator([]node.Emitter{srcNode}, opNode)
	subTopo.StoreSchema("rule1", "shared", map[string]*ast.JsonStreamField{
		"field1": {Type: "string"},
	}, false)
	assert.Equal(t, 1, len(subTopoPool))
	assert.Equal(t, srcNode, subTopo.GetSource())
	assert.Equal(t, []node.OperatorNode{opNode}, subTopo.ops)
	assert.Equal(t, opNode, subTopo.tail)
	assert.Equal(t, 1, subTopo.OpsCount())

	// Open is run asyncly, so Close may come first
	subTopo.Close(ctx)
	subTopo.Open(ctx, make(chan error))
	assert.Equal(t, 0, len(subTopo.refRules))
}

type mockSrc struct {
	name      string
	outputs   []chan<- any
	sentError bool
}

func (m *mockSrc) Broadcast(data interface{}) {
	// TODO implement me
	panic("implement me")
}

func (m *mockSrc) GetStreamContext() api.StreamContext {
	// TODO implement me
	panic("implement me")
}

func (m *mockSrc) SetQos(qos def.Qos) {
	// TODO implement me
	panic("implement me")
}

func (m *mockSrc) AddOutput(c chan interface{}, s string) error {
	m.outputs = append(m.outputs, c)
	return nil
}

func (m *mockSrc) RemoveOutput(s string) error {
	m.outputs = m.outputs[1:]
	return nil
}

func (m *mockSrc) Open(ctx api.StreamContext, errCh chan<- error) {
	if m.sentError {
		select {
		case errCh <- assert.AnError:
		default:
			fmt.Println("error is not sent")
		}
	}
}

func (m *mockSrc) GetName() string {
	return m.name
}

func (m *mockSrc) GetMetrics() []any {
	return []any{0, 0, 0, 0, 0, 0, 0, "", 0}
}

func (m *mockSrc) RemoveMetrics(ruleId string) {
	// do nothing
}

var _ checkpoint.StreamTask = &mockSrc{}

type mockOp struct {
	name        string
	ch          chan any
	outputs     []chan<- any
	inputC      int
	schemaCount int
}

func (m *mockOp) RemoveOutput(s string) error {
	if len(m.outputs) > 0 {
		m.outputs = m.outputs[1:]
	}
	return nil
}

func (m *mockOp) AddOutput(c chan interface{}, s string) error {
	m.outputs = append(m.outputs, c)
	return nil
}

func (m *mockOp) GetInput() (chan any, string) {
	return m.ch, m.name
}

func (m *mockOp) Exec(context api.StreamContext, errors chan<- error) {
	// do nothing
}

func (m *mockOp) GetName() string {
	return m.name
}

func (m *mockOp) GetMetrics() []any {
	return []any{0, 0, 0, 0, 0, 0, 0, "", 0}
}

func (m *mockOp) Broadcast(data interface{}) {
	// TODO implement me
	panic("implement me")
}

func (m *mockOp) GetStreamContext() api.StreamContext {
	// TODO implement me
	panic("implement me")
}

func (m *mockOp) GetInputCount() int {
	// TODO implement me
	panic("implement me")
}

func (m *mockOp) AddInputCount() {
	m.inputC++
}

func (m *mockOp) SetQos(qos def.Qos) {
	// TODO implement me
	panic("implement me")
}

func (m *mockOp) SetBarrierHandler(handler checkpoint.BarrierHandler) {
	// TODO implement me
	panic("implement me")
}

func (m *mockOp) RemoveMetrics(name string) {
	// do nothing
}

func (m *mockOp) ResetSchema(ctx api.StreamContext, schema map[string]*ast.JsonStreamField) {
	m.schemaCount = len(schema)
}

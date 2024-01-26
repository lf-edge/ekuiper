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

package topo

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	mockContext "github.com/lf-edge/ekuiper/internal/io/mock/context"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestSubtopoLC(t *testing.T) {
	assert.Equal(t, 0, mlen(&subTopoPool))
	subTopo, existed := GetSubTopo("shared")
	assert.False(t, existed)
	// Test creation
	srcNode := &mockSrc{name: "src1"}
	opNode := &mockOp{name: "op1", ch: make(chan any)}
	subTopo.AddSrc(srcNode)
	subTopo.AddOperator([]api.Emitter{srcNode}, opNode)
	assert.Equal(t, 1, mlen(&subTopoPool))
	assert.Equal(t, srcNode, subTopo.GetSource())
	assert.Equal(t, []node.OperatorNode{opNode}, subTopo.ops)
	assert.Equal(t, opNode, subTopo.tail)
	// Test linkage
	assert.Equal(t, 1, len(srcNode.outputs))
	var tch chan<- any = opNode.ch
	assert.Equal(t, tch, srcNode.outputs[0])
	ptopo := &api.PrintableTopo{
		Sources: []string{"subtopo_source_src1"},
		Edges: map[string][]any{
			"subtopo_source_src1": {"subtopo_op_op1"},
		},
	}
	assert.Equal(t, ptopo, subTopo.topo)
	// Test run
	subTopo.Open(mockContext.NewMockContext("rule1", "abc"), make(chan error))
	assert.Equal(t, int32(1), subTopo.refCount.Load())
	// Run another
	subTopo2, existed := GetSubTopo("shared")
	assert.True(t, existed)
	assert.Equal(t, subTopo, subTopo2)
	subTopo2.Open(mockContext.NewMockContext("rule2", "abc"), make(chan error))
	assert.Equal(t, int32(2), subTopo.refCount.Load())
	// Metrics test
	metrics := []any{0, 0, 0, 0, 0, 0, 0, "", 0, 0, 0, 0, 0, 0, 0, 0, "", 0}
	assert.Equal(t, metrics, subTopo.GetMetrics())
	keys := []string{"source_subtopo_src1_0_records_in_total", "source_subtopo_src1_0_records_out_total", "source_subtopo_src1_0_messages_processed_total", "source_subtopo_src1_0_process_latency_us", "source_subtopo_src1_0_buffer_length", "source_subtopo_src1_0_last_invocation", "source_subtopo_src1_0_exceptions_total", "source_subtopo_src1_0_last_exception", "source_subtopo_src1_0_last_exception_time", "op_subtopo_op1_0_records_in_total", "op_subtopo_op1_0_records_out_total", "op_subtopo_op1_0_messages_processed_total", "op_subtopo_op1_0_process_latency_us", "op_subtopo_op1_0_buffer_length", "op_subtopo_op1_0_last_invocation", "op_subtopo_op1_0_exceptions_total", "op_subtopo_op1_0_last_exception", "op_subtopo_op1_0_last_exception_time"}
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
	subTopo.RemoveMetrics("rule1")
	assert.Equal(t, int32(1), subTopo.refCount.Load())
	assert.Equal(t, 1, mlen(&subTopoPool))
	subTopo2.RemoveMetrics("rule2")
	assert.Equal(t, int32(0), subTopo.refCount.Load())
	assert.Equal(t, 0, mlen(&subTopoPool))
}

func TestSubtopoPrint(t *testing.T) {
	tt := &api.PrintableTopo{
		Sources: []string{"subtopo_source_src1"},
		Edges: map[string][]any{
			"subtopo_source_src1": {"subtopo_op_op1"},
		},
	}
	subTopo, _ := GetSubTopo("shared")
	subTopo.topo = tt
	subTopo.tail = &mockOp{name: "op1", ch: make(chan any)}
	ptopo := &api.PrintableTopo{
		Sources: []string{"mqtt_src1"},
		Edges:   map[string][]any{},
	}
	subTopo.MergeSrc(ptopo)
	assert.Equal(t, &api.PrintableTopo{
		Sources: []string{"mqtt_src1", "subtopo_source_src1"},
		Edges:   map[string][]any{"subtopo_source_src1": {"subtopo_op_op1"}},
	}, ptopo)
	subTopo.LinkTopo(ptopo, "project")
	assert.Equal(t, &api.PrintableTopo{
		Sources: []string{"mqtt_src1", "subtopo_source_src1"},
		Edges: map[string][]any{
			"subtopo_op_op1":      {"op_project"},
			"subtopo_source_src1": {"subtopo_op_op1"},
		},
	}, ptopo)
}

func mlen(m *sync.Map) int {
	var count int

	// Iterate through the map and count elements
	m.Range(func(key, value interface{}) bool {
		// Increment the counter for each element
		count++
		return true
	})
	return count
}

type mockSrc struct {
	name    string
	outputs []chan<- any
}

func (m *mockSrc) Broadcast(data interface{}) {
	// TODO implement me
	panic("implement me")
}

func (m *mockSrc) GetStreamContext() api.StreamContext {
	// TODO implement me
	panic("implement me")
}

func (m *mockSrc) SetQos(qos api.Qos) {
	// TODO implement me
	panic("implement me")
}

func (m *mockSrc) AddOutput(c chan<- interface{}, s string) error {
	m.outputs = append(m.outputs, c)
	return nil
}

func (m *mockSrc) Open(ctx api.StreamContext, errCh chan<- error) {
	// do nothing
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
	name    string
	ch      chan any
	outputs []chan<- any
	inputC  int
}

func (m *mockOp) AddOutput(c chan<- interface{}, s string) error {
	m.outputs = append(m.outputs, c)
	return nil
}

func (m *mockOp) GetInput() (chan<- any, string) {
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

func (m *mockOp) SetQos(qos api.Qos) {
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

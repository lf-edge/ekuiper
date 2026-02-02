// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package memory

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestSharedInmemoryNode(t *testing.T) {
	mockclock.ResetClock(100)
	pubsub.Reset()
	id := "test_id"
	sinkProps := make(map[string]interface{})
	sinkProps[pubsub.IdProperty] = id
	src := GetSource()
	snk := GetSink()
	contextLogger := conf.Log.WithField("rule", "test")
	ctx1 := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ctx, cancel := ctx1.WithCancel()
	srcProps := make(map[string]interface{})
	srcProps["option"] = "value"
	err := snk.Provision(ctx, sinkProps)
	if err != nil {
		t.Error(err)
		return
	}
	err = snk.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	if err != nil {
		t.Error(err)
		return
	}
	srcProps[pubsub.IdProperty] = id
	srcProps["datasource"] = id
	err = src.Provision(ctx, srcProps)
	if err != nil {
		t.Error(err)
	}

	rawTuple := &xsql.Tuple{
		Message:  map[string]any{"temp": 20},
		Metadata: nil,
	}
	mockclock.GetMockClock().Add(100)
	go func() {
		if gerr := snk.CollectList(ctx, &xsql.TransformedTupleList{Content: []api.MessageTuple{rawTuple}}); gerr != nil {
			t.Error(gerr)
		}
	}()
	err = src.Subscribe(ctx, func(ctx api.StreamContext, res any, meta map[string]any, ts time.Time) {
		expected := []pubsub.MemTuple{&xsql.Tuple{
			Emitter:   "",
			Timestamp: timex.GetNow(),
			Metadata:  map[string]any{"topic": id},
			Message:   rawTuple.Message,
		}}
		assert.Equal(t, expected, res)
		cancel()
	}, nil)
	assert.NoError(t, err)
	<-ctx.Done()
}

func TestUpdateListInmemoryNode(t *testing.T) {
	mockclock.ResetClock(100)
	pubsub.Reset()
	id := "test_id"
	sinkProps := map[string]any{
		"rowkindField": "update",
		"keyField":     "id",
	}
	sinkProps[pubsub.IdProperty] = id
	src := GetSource()
	snk := GetSink()
	contextLogger := conf.Log.WithField("rule", "test")
	ctx1 := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ctx, cancel := ctx1.WithCancel()
	srcProps := make(map[string]interface{})
	srcProps["option"] = "value"
	err := snk.Provision(ctx, sinkProps)
	if err != nil {
		t.Error(err)
		return
	}
	err = snk.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	if err != nil {
		t.Error(err)
		return
	}
	srcProps[pubsub.IdProperty] = id
	srcProps["datasource"] = id
	err = src.Provision(ctx, srcProps)
	if err != nil {
		t.Error(err)
	}

	rawTuple := &xsql.Tuple{
		Message:  map[string]any{"temp": 20, "id": 1, "update": "update"},
		Metadata: nil,
	}
	mockclock.GetMockClock().Add(100)
	go func() {
		if gerr := snk.CollectList(ctx, &xsql.TransformedTupleList{Content: []api.MessageTuple{rawTuple}}); gerr != nil {
			t.Error(gerr)
		}
	}()
	err = src.Subscribe(ctx, func(ctx api.StreamContext, res any, meta map[string]any, ts time.Time) {
		expected := []pubsub.MemTuple{&pubsub.UpdatableTuple{
			MemTuple: &xsql.Tuple{
				Emitter:   "",
				Timestamp: timex.GetNow(),
				Metadata:  map[string]any{"topic": id},
				Message:   rawTuple.Message,
			},
			Rowkind: "update",
			Keyval:  1,
		}}
		assert.Equal(t, expected, res)
		cancel()
	}, nil)
	assert.NoError(t, err)
	<-ctx.Done()
}

func TestUpdateInmemoryNode(t *testing.T) {
	mockclock.ResetClock(100)
	pubsub.Reset()
	id := "test_id"
	sinkProps := map[string]any{
		"rowkindField": "update",
		"keyField":     "id",
	}
	sinkProps[pubsub.IdProperty] = id
	src := GetSource()
	snk := GetSink()
	contextLogger := conf.Log.WithField("rule", "test")
	ctx1 := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ctx, cancel := ctx1.WithCancel()
	srcProps := make(map[string]interface{})
	srcProps["option"] = "value"
	err := snk.Provision(ctx, sinkProps)
	if err != nil {
		t.Error(err)
		return
	}
	err = snk.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	if err != nil {
		t.Error(err)
		return
	}
	srcProps[pubsub.IdProperty] = id
	srcProps["datasource"] = id
	err = src.Provision(ctx, srcProps)
	if err != nil {
		t.Error(err)
	}

	rawTuple := &xsql.Tuple{
		Message:  map[string]any{"temp": 20, "id": 1, "update": "delete"},
		Metadata: nil,
	}
	mockclock.GetMockClock().Add(100)
	go func() {
		if gerr := snk.Collect(ctx, rawTuple); gerr != nil {
			t.Error(gerr)
		}
	}()
	err = src.Subscribe(ctx, func(ctx api.StreamContext, res any, meta map[string]any, ts time.Time) {
		expected := &pubsub.UpdatableTuple{
			MemTuple: &xsql.Tuple{
				Emitter:   "",
				Timestamp: timex.GetNow(),
				Metadata:  map[string]any{"topic": id},
				Message:   rawTuple.Message,
			},
			Rowkind: "delete",
			Keyval:  1,
		}
		assert.Equal(t, expected, res)
		cancel()
	}, nil)
	assert.NoError(t, err)
	<-ctx.Done()
}

func TestMultipleTopics(t *testing.T) {
	pubsub.Reset()
	var (
		sourceTopics = []string{"h/d1/c1/s2", "h/+/+/s1", "h/d3/#", "h/d1/c1/s2", "h/+/c1/s1"}
		sinkTopics   = []string{"h/d1/c1/s1", "h/d1/c1/s2", "h/d2/c2/s1", "h/d3/c3/s1"}
		sinkData     = [][]map[string]interface{}{
			{
				{
					"id":   1,
					"temp": 23,
				},
				{
					"id":   2,
					"temp": 34,
				},
				{
					"id":   3,
					"temp": 28,
				},
			}, {
				{
					"id":    4,
					"color": "red",
				},
				{
					"id":    5,
					"color": "red",
				},
				{
					"id":    6,
					"color": "green",
				},
			}, {
				{
					"id":  7,
					"hum": 67.5,
				},
				{
					"id":  8,
					"hum": 77.1,
				},
				{
					"id":  9,
					"hum": 90.3,
				},
			}, {
				{
					"id":     10,
					"status": "on",
				},
				{
					"id":     11,
					"status": "off",
				},
				{
					"id":     12,
					"status": "on",
				},
			},
		}
		expected = [][]*xsql.Tuple{
			{ // 0 "h/d1/c1/s2",
				{Message: map[string]any{"id": 4, "color": "red"}, Metadata: map[string]any{"topic": "h/d1/c1/s2"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 5, "color": "red"}, Metadata: map[string]any{"topic": "h/d1/c1/s2"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 6, "color": "green"}, Metadata: map[string]any{"topic": "h/d1/c1/s2"}, Timestamp: timex.GetNow()},
			},
			{ // 1 "h/+/+/s1",
				{Message: map[string]any{"id": 1, "temp": 23}, Metadata: map[string]any{"topic": "h/d1/c1/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 2, "temp": 34}, Metadata: map[string]any{"topic": "h/d1/c1/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 3, "temp": 28}, Metadata: map[string]any{"topic": "h/d1/c1/s1"}, Timestamp: timex.GetNow()},

				{Message: map[string]any{"id": 7, "hum": 67.5}, Metadata: map[string]any{"topic": "h/d2/c2/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 8, "hum": 77.1}, Metadata: map[string]any{"topic": "h/d2/c2/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 9, "hum": 90.3}, Metadata: map[string]any{"topic": "h/d2/c2/s1"}, Timestamp: timex.GetNow()},

				{Message: map[string]any{"id": 10, "status": "on"}, Metadata: map[string]any{"topic": "h/d3/c3/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 11, "status": "off"}, Metadata: map[string]any{"topic": "h/d3/c3/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 12, "status": "on"}, Metadata: map[string]any{"topic": "h/d3/c3/s1"}, Timestamp: timex.GetNow()},
			},
			{ // 2 "h/d3/#",
				{Message: map[string]any{"id": 10, "status": "on"}, Metadata: map[string]any{"topic": "h/d3/c3/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 11, "status": "off"}, Metadata: map[string]any{"topic": "h/d3/c3/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 12, "status": "on"}, Metadata: map[string]any{"topic": "h/d3/c3/s1"}, Timestamp: timex.GetNow()},
			},
			{ // 3 "h/d1/c1/s2",
				{Message: map[string]any{"id": 4, "color": "red"}, Metadata: map[string]any{"topic": "h/d1/c1/s2"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 5, "color": "red"}, Metadata: map[string]any{"topic": "h/d1/c1/s2"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 6, "color": "green"}, Metadata: map[string]any{"topic": "h/d1/c1/s2"}, Timestamp: timex.GetNow()},
			},
			{ // 4 "h/+/c1/s1"
				{Message: map[string]any{"id": 1, "temp": 23}, Metadata: map[string]any{"topic": "h/d1/c1/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 2, "temp": 34}, Metadata: map[string]any{"topic": "h/d1/c1/s1"}, Timestamp: timex.GetNow()},
				{Message: map[string]any{"id": 3, "temp": 28}, Metadata: map[string]any{"topic": "h/d1/c1/s1"}, Timestamp: timex.GetNow()},
			},
		}
	)

	contextLogger := conf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	// create pub
	for _, topic := range sinkTopics {
		snk := GetSink()
		err := snk.Provision(ctx, map[string]interface{}{"topic": topic})
		if err != nil {
			t.Error(err)
			return
		}
		err = snk.Connect(ctx, func(status string, message string) {
			// do nothing
		})
		if err != nil {
			t.Error(err)
			return
		}
	}
	// receive data
	var wg sync.WaitGroup
	for i, topic := range sourceTopics {
		wg.Add(1)
		src := GetSource()
		err := src.Provision(ctx, map[string]any{"datasource": topic})
		assert.NoError(t, err)
		limit := len(expected[i])
		result := make([]*xsql.Tuple, 0, limit)
		nc, cancel := ctx.WithMeta("rule1", fmt.Sprintf("op%d", i), &state.MemoryStore{}).WithCancel()
		err = src.Subscribe(nc, func(ctx api.StreamContext, res any, meta map[string]any, ts time.Time) {
			rid, _ := res.(*xsql.Tuple).Message["id"]
			fmt.Printf("%d(%s) receive %v\n", i, topic, rid)
			result = append(result, res.(*xsql.Tuple))
			limit--
			if limit == 0 {
				assert.Equal(t, result, expected[i], i)
				cancel()
				wg.Done()
			}
		}, nil)
		assert.NoError(t, err)
	}

	for i, v := range sinkData {
		topic := sinkTopics[i]
		for _, mm := range v {
			time.Sleep(10 * time.Millisecond)
			pubsub.Produce(ctx, topic, &xsql.Tuple{Message: mm, Metadata: map[string]any{"topic": topic}, Timestamp: timex.GetNow()})
			fmt.Printf("send to topic %s: %v\n", topic, mm["id"])
		}

	}
	wg.Wait()
}

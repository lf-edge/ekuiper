// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"github.com/gdexlab/go-render/render"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"testing"
	"time"
)

func TestSharedInmemoryNode(t *testing.T) {
	pubsub.Reset()
	id := "test_id"
	sinkProps := make(map[string]interface{})
	sinkProps[pubsub.IdProperty] = id
	src := GetSource()
	snk := GetSink()
	contextLogger := conf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	consumer := make(chan api.SourceTuple)
	errorChannel := make(chan error)
	srcProps := make(map[string]interface{})
	srcProps["option"] = "value"
	err := snk.Configure(sinkProps)
	if err != nil {
		t.Error(err)
		return
	}
	err = snk.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	srcProps[pubsub.IdProperty] = id
	err = src.Configure(id, srcProps)
	if err != nil {
		t.Error(err)
	}
	go func() {
		cc := ctx.WithMeta("rule", fmt.Sprintf("test"), &state.MemoryStore{})
		src.Open(cc, consumer, errorChannel)
	}()

	//if _, contains := pubTopics[id]; !contains {
	//	t.Errorf("there should be memory node for topic")
	//}

	data := make(map[string]interface{})
	data["temperature"] = 33.0
	list := make([]map[string]interface{}, 0)
	list = append(list, data)
	go func() {
		err = snk.Collect(ctx, list)
		if err != nil {
			t.Error(err)
		}
	}()
	for {
		select {
		case res := <-consumer:
			expected := api.NewDefaultSourceTuple(data, map[string]interface{}{"topic": "test_id"})
			if !reflect.DeepEqual(expected, res) {
				t.Errorf("result %s should be equal to %s", res, expected)
			}
			return
		default:
		}
	}
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
		expected = []api.SourceTuple{
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":   1,
					"temp": 23,
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":   1,
					"temp": 23,
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":    4,
					"color": "red",
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s2",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":    4,
					"color": "red",
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s2",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":  7,
					"hum": 67.5,
				},
				M: map[string]interface{}{
					"topic": "h/d2/c2/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":     10,
					"status": "on",
				},
				M: map[string]interface{}{
					"topic": "h/d3/c3/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":     10,
					"status": "on",
				},
				M: map[string]interface{}{
					"topic": "h/d3/c3/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":   2,
					"temp": 34,
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":   2,
					"temp": 34,
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":    5,
					"color": "red",
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s2",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":    5,
					"color": "red",
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s2",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":  8,
					"hum": 77.1,
				},
				M: map[string]interface{}{
					"topic": "h/d2/c2/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":     11,
					"status": "off",
				},
				M: map[string]interface{}{
					"topic": "h/d3/c3/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":     11,
					"status": "off",
				},
				M: map[string]interface{}{
					"topic": "h/d3/c3/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":   3,
					"temp": 28,
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":   3,
					"temp": 28,
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":    6,
					"color": "green",
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s2",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":    6,
					"color": "green",
				},
				M: map[string]interface{}{
					"topic": "h/d1/c1/s2",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":  9,
					"hum": 90.3,
				},
				M: map[string]interface{}{
					"topic": "h/d2/c2/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":     12,
					"status": "on",
				},
				M: map[string]interface{}{
					"topic": "h/d3/c3/s1",
				},
			},
			&api.DefaultSourceTuple{
				Mess: map[string]interface{}{
					"id":     12,
					"status": "on",
				},
				M: map[string]interface{}{
					"topic": "h/d3/c3/s1",
				},
			},
		}
	)

	contextLogger := conf.Log.WithField("rule", "test")
	ctx, cancel := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithCancel()
	consumer := make(chan api.SourceTuple)
	errorChannel := make(chan error)

	count := 0
	for _, topic := range sinkTopics {
		snk := GetSink()
		err := snk.Configure(map[string]interface{}{"topic": topic})
		if err != nil {
			t.Error(err)
			return
		}
		err = snk.Open(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		src := GetSource()
		err = src.Configure(sourceTopics[count], make(map[string]interface{}))
		if err != nil {
			t.Error(err)
			return
		}
		go func(c int) {
			nc := ctx.WithMeta("rule1", fmt.Sprintf("op%d", c), &state.MemoryStore{})
			src.Open(nc, consumer, errorChannel)
		}(count)
		count++
	}
	for count < len(sourceTopics) {
		src := GetSource()
		err := src.Configure(sourceTopics[count], make(map[string]interface{}))
		if err != nil {
			t.Error(err)
			return
		}
		go func(c int) {
			nc := ctx.WithMeta("rule1", fmt.Sprintf("op%d", c), &state.MemoryStore{})
			src.Open(nc, consumer, errorChannel)
		}(count)
		count++
	}

	go func() {
		c := 0
		for c < 3 {
			for i, v := range sinkData {
				time.Sleep(10 * time.Millisecond)
				pubsub.Produce(ctx, sinkTopics[i], v[c])
			}
			c++
		}
		cancel()
		time.Sleep(100 * time.Millisecond)
		close(consumer)
	}()
	var results []api.SourceTuple
	for res := range consumer {
		results = append(results, res)
	}
	if !reflect.DeepEqual(expected, results) {
		t.Errorf("Expect\t %v\n but got\t\t\t\t %v", render.AsCode(expected), render.AsCode(results))
	}
}

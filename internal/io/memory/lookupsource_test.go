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
	gocontext "context"
	"reflect"
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestUpdateLookup(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ls := GetLookupSource()
	err := ls.Configure("test", map[string]interface{}{"key": "ff"})
	if err != nil {
		t.Error(err)
		return
	}
	err = ls.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	// wait for the source to be ready
	time.Sleep(100 * time.Millisecond)
	pubsub.Produce(ctx, "test", map[string]interface{}{"ff": "value1", "gg": "value2"})
	pubsub.ProduceUpdatable(ctx, "test", map[string]interface{}{"ff": "value1", "gg": "value2"}, "delete", "value1")
	pubsub.ProduceUpdatable(ctx, "test", map[string]interface{}{"ff": "value2", "gg": "value2"}, "insert", "value2")
	pubsub.Produce(ctx, "test", map[string]interface{}{"ff": "value1", "gg": "value4"})
	pubsub.ProduceUpdatable(ctx, "test", map[string]interface{}{"ff": "value2", "gg": "value2"}, "delete", "value2")
	pubsub.Produce(ctx, "test", map[string]interface{}{"ff": "value1", "gg": "value2"})
	pubsub.Produce(ctx, "test", map[string]interface{}{"ff": "value2", "gg": "value2"})
	// wait for table accumulation
	time.Sleep(100 * time.Millisecond)
	canctx, cancel := gocontext.WithCancel(gocontext.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-canctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				pubsub.Produce(ctx, "test", map[string]interface{}{"ff": "value4", "gg": "value2"})
			}
		}
	}()
	mc := conf.Clock.(*clock.Mock)
	expected := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"ff": "value1", "gg": "value2"}, map[string]interface{}{"topic": "test"}, mc.Now()),
	}
	result, err := ls.Lookup(ctx, []string{}, []string{"ff"}, []interface{}{"value1"})
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expect %v but got %v", expected, result)
	}
	err = ls.Close(ctx)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestLookup(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "test2")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ls := GetLookupSource()
	err := ls.Configure("test2", map[string]interface{}{"key": "gg"})
	if err != nil {
		t.Error(err)
		return
	}
	err = ls.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	// wait for the source to be ready
	time.Sleep(100 * time.Millisecond)
	pubsub.Produce(ctx, "test2", map[string]interface{}{"ff": "value1", "gg": "value2"})
	pubsub.Produce(ctx, "test2", map[string]interface{}{"ff": "value2", "gg": "value3"})
	pubsub.Produce(ctx, "test2", map[string]interface{}{"ff": "value1", "gg": "value4"})
	// wait for table accumulation
	time.Sleep(100 * time.Millisecond)
	canctx, cancel := gocontext.WithCancel(gocontext.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-canctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				pubsub.Produce(ctx, "test", map[string]interface{}{"ff": "value4", "gg": "value5"})
			}
		}
	}()
	mc := conf.Clock.(*clock.Mock)
	expected := []api.SourceTuple{
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"ff": "value1", "gg": "value2"}, map[string]interface{}{"topic": "test2"}, mc.Now()),
		api.NewDefaultSourceTupleWithTime(map[string]interface{}{"ff": "value1", "gg": "value4"}, map[string]interface{}{"topic": "test2"}, mc.Now()),
	}
	result, _ := ls.Lookup(ctx, []string{}, []string{"ff"}, []interface{}{"value1"})
	if len(result) != 2 {
		t.Errorf("expect %v but got %v", expected, result)
	} else {
		if result[0].Message()["gg"] != "value2" {
			result[0], result[1] = result[1], result[0]
		}
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expect %v but got %v", expected, result)
	}
	err = ls.Close(ctx)
	if err != nil {
		t.Error(err)
		return
	}
}

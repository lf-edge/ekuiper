// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/memory/pubsub"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"testing"
	"time"
)

func TestSingleKeyLookup(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ls := GetLookupSource()
	err := ls.Configure("test", map[string]interface{}{"option": "value"})
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
	pubsub.Produce(ctx, "test", map[string]interface{}{"ff": "value2", "gg": "value2"})
	pubsub.Produce(ctx, "test", map[string]interface{}{"ff": "value1", "gg": "value4"})
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
	expected := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"ff": "value1", "gg": "value2"}, map[string]interface{}{"topic": "test"}),
		api.NewDefaultSourceTuple(map[string]interface{}{"ff": "value1", "gg": "value4"}, map[string]interface{}{"topic": "test"}),
	}
	result, err := ls.Lookup(ctx, []string{"ff"}, []interface{}{"value1"})
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expect %v but got %v", expected, result)
	}
	err = ls.Close(ctx)
	if err != nil {
		t.Error(err)
		return
	}
}

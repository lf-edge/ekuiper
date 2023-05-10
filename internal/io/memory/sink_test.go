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
	"reflect"
	"testing"

	"github.com/benbjohnson/clock"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestUpdate(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "test2")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	ms := GetSink()
	err := ms.Configure(map[string]interface{}{"topic": "testupdate", "rowkindField": "verb", "keyField": "id"})
	if err != nil {
		t.Error(err)
		return
	}
	err = ms.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var data = []map[string]interface{}{
		{"id": "1", "verb": "insert", "name": "test1"},
		{"id": "2", "verb": "insert", "name": "test2"},
		{"id": "1", "verb": "update", "name": "test1"},
		{"id": "2", "verb": "delete", "name": "test2"},
	}
	c := pubsub.CreateSub("testupdate", nil, "testSource", 100)
	go func() {
		for _, d := range data {
			ms.Collect(ctx, d)
		}
	}()
	var actual []api.SourceTuple
	for i := 0; i < 4; i++ {
		d := <-c
		fmt.Println(d)
		actual = append(actual, d)
	}
	mc := conf.Clock.(*clock.Mock)
	expects := []api.SourceTuple{
		&pubsub.UpdatableTuple{
			DefaultSourceTuple: api.NewDefaultSourceTupleWithTime(map[string]interface{}{"id": "1", "verb": "insert", "name": "test1"}, map[string]interface{}{"topic": "testupdate"}, mc.Now()),
			Rowkind:            "insert",
			Keyval:             "1",
		},
		&pubsub.UpdatableTuple{
			DefaultSourceTuple: api.NewDefaultSourceTupleWithTime(map[string]interface{}{"id": "2", "verb": "insert", "name": "test2"}, map[string]interface{}{"topic": "testupdate"}, mc.Now()),
			Rowkind:            "insert",
			Keyval:             "2",
		},
		&pubsub.UpdatableTuple{
			DefaultSourceTuple: api.NewDefaultSourceTupleWithTime(map[string]interface{}{"id": "1", "verb": "update", "name": "test1"}, map[string]interface{}{"topic": "testupdate"}, mc.Now()),
			Rowkind:            "update",
			Keyval:             "1",
		},
		&pubsub.UpdatableTuple{
			DefaultSourceTuple: api.NewDefaultSourceTupleWithTime(map[string]interface{}{"id": "2", "verb": "delete", "name": "test2"}, map[string]interface{}{"topic": "testupdate"}, mc.Now()),
			Rowkind:            "delete",
			Keyval:             "2",
		},
	}
	if !reflect.DeepEqual(actual, expects) {
		t.Errorf("expect %v but got %v", expects, actual)
	}
}

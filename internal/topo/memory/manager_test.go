// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"reflect"
	"testing"
)

func TestSharedInmemoryNode(t *testing.T) {

	id := "test_id"
	sinkProps := make(map[string]interface{})
	sinkProps[IdProperty] = id
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
	go func() {
		src.Open(ctx, consumer, errorChannel)
	}()
	err = src.Configure(id, srcProps)
	if err != nil {
		t.Error(err)
	}

	srcProps[IdProperty] = id

	if _, contains := pubTopics[id]; !contains {
		t.Errorf("there should be memory node for topic")
	}

	data := make(map[string]interface{})
	data["temperature"] = 33.0
	list := make([]map[string]interface{}, 0)
	list = append(list, data)
	go func() {
		var buf []byte
		buf, err = asJsonBytes(list)
		if err != nil {
			t.Error(err)
		}
		err = snk.Collect(ctx, buf)
		if err != nil {
			t.Error(err)
		}
	}()
	for {
		select {
		case res := <-consumer:
			expected := api.NewDefaultSourceTuple(data, make(map[string]interface{}))
			if !reflect.DeepEqual(expected, res) {
				t.Errorf("result %s should be equal to %s", res, expected)
			}
			return
		default:
		}
	}
}

func asJsonBytes(m []map[string]interface{}) ([]byte, error) {
	return json.Marshal(m)
}

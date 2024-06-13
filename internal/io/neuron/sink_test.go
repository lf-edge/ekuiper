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

package neuron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
)

func TestSink(t *testing.T) {
	server, ch := mockNeuron(false, true, DefaultNeuronUrl)
	defer server.Close()

	s := GetSink().(api.TupleCollector)
	data := []any{
		&xsql.Tuple{
			Message: map[string]any{
				"temperature": 22,
				"humidity":    50,
				"status":      "green",
			},
		},
		&xsql.Tuple{
			Message: map[string]any{
				"temperature": 25,
				"humidity":    82,
				"status":      "wet",
			},
		},
		&xsql.Tuple{
			Message: map[string]any{
				"temperature": 33,
				"humidity":    60,
				"status":      "hot",
			},
		},
	}
	err := mock.RunTupleSinkCollect(s, data, map[string]any{
		"url":       DefaultNeuronUrl,
		"nodeName":  "test1",
		"groupName": "grp",
		"tags":      []string{"temperature", "status"},
		"raw":       false,
	})
	assert.NoError(t, err)

	exp := []string{
		`{"group_name":"grp","node_name":"test1","tag_name":"temperature","value":22}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"status","value":"green"}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"temperature","value":25}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"status","value":"wet"}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"temperature","value":33}`,
		`{"group_name":"grp","node_name":"test1","tag_name":"status","value":"hot"}`,
	}
	var actual []string
	ticker := time.After(5 * time.Second)
	for i := 0; i < len(exp); i++ {
		select {
		case <-ticker:
			t.Errorf("timeout")
			return
		case d := <-ch:
			actual = append(actual, string(d))
		}
	}

	assert.Equal(t, exp, actual)
	time.Sleep(100 * time.Millisecond)
}

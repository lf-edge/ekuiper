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

package cast

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type d struct {
	Duration DurationConf `json:"duration" yaml:"duration"`
	Id       int          `json:"id" yaml:"id"`
}

func TestJsonUnmarshall(t *testing.T) {
	tests := []struct {
		name string
		str  string
		r    d
		e    string
	}{
		{
			name: "normal",
			str:  `{"duration": "10s","id":20}`,
			r:    d{Duration: DurationConf(10 * time.Second), Id: 20},
		},
		{
			name: "wrong duration string",
			str:  `{"duration": "10","id":20}`,
			e:    "time: missing unit in duration \"10\"",
		},
		{
			name: "duration int",
			str:  `{"duration": 10,"id":20}`,
			r:    d{Duration: DurationConf(10 * time.Millisecond), Id: 20},
		},
		{
			name: "duration missing",
			str:  `{"id":20}`,
			r:    d{Duration: DurationConf(time.Second), Id: 20},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := d{
				Duration: DurationConf(time.Second),
			}
			err := json.Unmarshal([]byte(tt.str), &r)
			if tt.e != "" {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.e)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.r, r)
			}
		})
	}
}

func TestJsonMarshall(t *testing.T) {
	tests := []struct {
		name string
		str  string
		r    d
	}{
		{
			name: "normal",
			str:  `{"duration":"10s","id":20}`,
			r:    d{Duration: DurationConf(10 * time.Second), Id: 20},
		},
		{
			name: "duration int",
			str:  `{"duration":"10ms","id":20}`,
			r:    d{Duration: DurationConf(10 * time.Millisecond), Id: 20},
		},
		{
			name: "duration missing",
			str:  `{"duration":"1s","id":20}`,
			r:    d{Duration: DurationConf(time.Second), Id: 20},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := json.Marshal(tt.r)
			assert.NoError(t, err)
			assert.Equal(t, tt.str, string(s))
		})
	}
}

func TestYamlUnmarshall(t *testing.T) {
	tests := []struct {
		name string
		str  string
		r    d
		e    string
	}{
		{
			name: "normal",
			str:  `{"duration": "10s","id":20}`,
			r:    d{Duration: DurationConf(10 * time.Second), Id: 20},
		},
		{
			name: "wrong duration string",
			str:  `{"duration": "10","id":20}`,
			e:    "time: missing unit in duration \"10\"",
		},
		{
			name: "duration int",
			str:  `{"duration": 10,"id":20}`,
			r:    d{Duration: DurationConf(10 * time.Millisecond), Id: 20},
		},
		{
			name: "duration missing",
			str:  `{"id":20}`,
			r:    d{Duration: DurationConf(time.Second), Id: 20},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := d{
				Duration: DurationConf(time.Second),
			}
			err := yaml.Unmarshal([]byte(tt.str), &r)
			if tt.e != "" {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.e)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.r, r)
			}
		})
	}
}

func TestYamlMarshall(t *testing.T) {
	tests := []struct {
		name string
		str  string
		r    d
	}{
		{
			name: "normal",
			str:  "duration: 10s\nid: 20\n",
			r:    d{Duration: DurationConf(10 * time.Second), Id: 20},
		},
		{
			name: "duration int",
			str:  "duration: 10ms\nid: 20\n",
			r:    d{Duration: DurationConf(10 * time.Millisecond), Id: 20},
		},
		{
			name: "duration missing",
			str:  "duration: 1s\nid: 20\n",
			r:    d{Duration: DurationConf(time.Second), Id: 20},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := yaml.Marshal(tt.r)
			assert.NoError(t, err)
			assert.Equal(t, tt.str, string(s))
		})
	}
}

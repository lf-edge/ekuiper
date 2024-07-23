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

package zmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		n string
		p map[string]any
		c *c
		e string
	}{
		{
			n: "normal",
			p: map[string]any{
				"server":     "tcp://127.0.0.1:5563",
				"datasource": "t1",
			},
			c: &c{
				Server: "tcp://127.0.0.1:5563",
				Topic:  "t1",
			},
		},
		{
			n: "wrong type",
			p: map[string]any{
				"server":     "tcp://127.0.0.1:5563",
				"datasource": 1,
			},
			e: "1 error(s) decoding:\n\n* 'datasource' expected type 'string', got unconvertible type 'int', value: '1'",
		},
		{
			n: "missing server",
			p: map[string]any{},
			e: "missing server address",
		},
	}
	for _, test := range tests {
		t.Run(test.n, func(t *testing.T) {
			r, err := validate(nil, test.p)
			if test.e != "" {
				assert.EqualError(t, err, test.e)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.c, r)
			}
		})
	}
}

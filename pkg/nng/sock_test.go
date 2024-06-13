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

package nng

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name: "wrong format",
			props: map[string]any{
				"url": 345,
			},
			err: "1 error(s) decoding:\n\n* 'url' expected type 'string', got unconvertible type 'int', value: '345'",
		},
		{
			name:  "missing url",
			props: map[string]any{},
			err:   "url is required",
		},
		{
			name: "wrong url",
			props: map[string]any{
				"url": "file:////abc",
			},
			err: "only tcp and ipc scheme are supported",
		},
		{
			name: "wrong protocol",
			props: map[string]any{
				"url":      "tcp://127.0.0.1:444",
				"protocol": "pair1",
			},
			err: "unsupported protocol pair1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, e := ValidateConf(tt.props)
			assert.Error(t, e)
			assert.EqualError(t, e, tt.err)
		})
	}
}

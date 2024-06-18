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

package image

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigure(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		c     *c
		err   string
	}{
		{
			name: "wrong type",
			props: map[string]any{
				"maxAge": "0.11",
			},
			err: "1 error(s) decoding:\n\n* 'maxAge' expected type 'int', got unconvertible type 'string', value: '0.11'",
		},
		{
			name: "missing path",
			props: map[string]any{
				"imageFormat": "jpeg",
			},
			err: "path is required",
		},
		{
			name: "wrong format",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "abc",
			},
			err: "abc image type is not currently supported",
		},
		{
			name: "default age",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
				"maxCount":    1,
			},
			c: &c{
				Path:        "data",
				ImageFormat: "png",
				MaxCount:    1,
				MaxAge:      72,
			},
		},
		{
			name: "default count",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
				"maxAge":      0.11,
			},
			c: &c{
				Path:        "data",
				ImageFormat: "png",
				MaxCount:    1000,
				MaxAge:      0,
			},
		},
	}
	s := &imageSink{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.Configure(test.props)
			if test.err == "" {
				assert.NoError(t, err)
				assert.Equal(t, test.c, s.c)
			} else {
				assert.EqualError(t, err, test.err)
			}
		})
	}
}

func TestSave(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		image string
		err   string
	}{
		{
			name: "normal",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
			},
			image: "../../../../docs/en_US/wechat.png",
		},
		{
			name: "wrong format",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "jpeg",
			},
			image: "../../../../docs/en_US/wechat.png",
			err:   "invalid JPEG format: missing SOI marker",
		},
		{
			name: "normal jpeg",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "jpeg",
			},
			image: "ekuiper.jpg",
		},
		{
			name: "wrong png",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
			},
			image: "ekuiper.jpg",
			err:   "png: invalid format: not a PNG file",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.MkdirAll("data", os.ModePerm)
			assert.NoError(t, err)
			b, err := os.ReadFile(tt.image)
			assert.NoError(t, err)
			s := &imageSink{}
			err = s.Configure(tt.props)
			assert.NoError(t, err)

			err = s.saveFiles(map[string]any{
				"self": b,
			})
			if tt.err == "" {
				assert.NoError(t, err)
				entries, err := os.ReadDir("data")
				assert.NoError(t, err)
				assert.Len(t, entries, 1)
			} else {
				assert.EqualError(t, err, tt.err)
				entries, err := os.ReadDir("data")
				assert.NoError(t, err)
				assert.Len(t, entries, 0)
			}
			_ = os.RemoveAll("data")
		})
	}
}

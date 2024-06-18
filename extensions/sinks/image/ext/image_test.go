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

func TestSave(t *testing.T) {
	tests := []struct {
		name  string
		sink  *imageSink
		image string
		err   string
	}{
		{
			name: "normal",
			sink: &imageSink{
				path:     "data",
				format:   "png",
				maxAge:   0,
				maxCount: 0,
			},
			image: "../../../../docs/en_US/wechat.png",
		},
		{
			name: "wrong format",
			sink: &imageSink{
				path:     "data",
				format:   "jpeg",
				maxAge:   0,
				maxCount: 0,
			},
			image: "../../../../docs/en_US/wechat.png",
			err:   "invalid JPEG format: missing SOI marker",
		},
		{
			name: "normal jpeg",
			sink: &imageSink{
				path:     "data",
				format:   "jpeg",
				maxAge:   0,
				maxCount: 0,
			},
			image: "ekuiper.jpg",
		},
		{
			name: "wrong png",
			sink: &imageSink{
				path:     "data",
				format:   "png",
				maxAge:   0,
				maxCount: 0,
			},
			image: "ekuiper.jpg",
			err:   "png: invalid format: not a PNG file",
		},
		{
			name: "unsupported format",
			sink: &imageSink{
				path:     "data",
				format:   "abc",
				maxAge:   0,
				maxCount: 0,
			},
			image: "../../../../docs/cover.jpg",
			err:   "unsupported format abc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.MkdirAll("data", os.ModePerm)
			assert.NoError(t, err)
			b, err := os.ReadFile(tt.image)
			assert.NoError(t, err)
			err = tt.sink.saveFiles(map[string]any{
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
			//_ = os.RemoveAll("data")
		})
	}
}

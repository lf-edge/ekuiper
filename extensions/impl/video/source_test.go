// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package video

import (
	"errors"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestProvision(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		err   string
	}{
		{
			name: "wrong param type",
			props: map[string]any{
				"url": 45,
			},
			err: "",
		},
		{
			name:  "missing url",
			props: map[string]any{},
			err:   "missing url",
		},
		{
			name: "wrong url",
			props: map[string]any{
				"url": "rtsp/dafsa",
			},
			err: "wrong url",
		},
	}
	ctx := mockContext.NewMockContext("test", "test")
	s := GetSource()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.Provision(ctx, test.props)
			assert.Error(t, err)
		})
	}
}

func TestPull(t *testing.T) {
	meta := map[string]any{
		"url": "https://hdgcwbcdali.v.myalicdn.com/hdgcwbcd/cdrmipanda1000_1/index.m3u8",
	}
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple(nil, meta, timex.GetNow()),
	}
	r := GetSource()
	mock.TestSourceConnectorCompare(t, r, map[string]any{
		"url":       "https://hdgcwbcdali.v.myalicdn.com/hdgcwbcd/cdrmipanda1000_1/index.m3u8",
		"interval":  "15s",
		"debugResp": true,
		"inputArgs": map[string]any{
			"user_agent": "test_agent",
		},
	}, exp, func(e any, r any) bool {
		et, ok := e.([]api.MessageTuple)
		b := assert.True(t, ok)
		rt, ok := r.([]api.MessageTuple)
		b = b && assert.True(t, ok, "result is not []api.MessageTuple")
		b = b && assert.Equal(t, len(et), len(rt))
		for i := range et {
			rti := rt[i].(*model.DefaultSourceTuple)
			b = b && assert.True(t, len(rti.Raw()) > 100)
		}
		return b
	}, func() {
		// do nothing
	})
}

func TestPullError(t *testing.T) {
	exp := errors.New("read frame failed, err:exit status 8")
	r := GetSource()
	mock.TestSourceConnector(t, r, map[string]any{
		"url":      "https://hdgcwbcdali.v.myalicdn.com/hdgcwbcd/cdrmipanda1000_1/index.m3u8",
		"codec":    "ogv",
		"interval": "15s",
	}, exp, func() {
		// do nothing
	})
}

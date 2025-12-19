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
	"bytes"
	"fmt"
	"image/jpeg"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestMain(m *testing.M) {
	// Generate mock video file
	_ = os.Remove("test.mp4")
	cmd := exec.Command("ffmpeg", "-f", "lavfi", "-i", "testsrc=duration=5:size=640x480:rate=10", "-vcodec", "libx264", "-y", "test.mp4")
	if err := cmd.Run(); err != nil {
		fmt.Printf("failed to generate test.mp4: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.Remove("test.mp4")
	os.Exit(code)
}

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

func TestSubscribe(t *testing.T) {
	conf.Log.SetOutput(os.Stdout)
	pwd, _ := os.Getwd()
	testFile := "file://" + filepath.Join(pwd, "test.mp4")
	meta := map[string]any{
		"url": testFile,
	}
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple(nil, meta, timex.GetNow()),
	}
	r := GetSource()
	mock.TestSourceConnectorCompare(t, r, map[string]any{
		"url":       testFile,
		"interval":  "1s",
		"debugResp": true,
	}, exp, func(e any, r any) bool {
		et, ok := e.([]api.MessageTuple)
		assert.True(t, ok)
		rt, ok := r.([]api.MessageTuple)
		assert.True(t, ok, "result is not []api.MessageTuple")
		if !assert.Equal(t, len(et), len(rt)) {
			return false
		}
		b := true
		for i := range et {
			rti, ok := rt[i].(*model.DefaultSourceTuple)
			if !assert.True(t, ok, "item %d is not *model.DefaultSourceTuple", i) {
				return false
			}
			raw := rti.Raw()
			b = b && assert.True(t, len(raw) > 100)
			cfg, err := jpeg.DecodeConfig(bytes.NewReader(raw))
			if !assert.NoError(t, err, "item %d is not a valid jpeg", i) {
				return false
			}
			b = b && assert.Equal(t, 640, cfg.Width)
			b = b && assert.Equal(t, 480, cfg.Height)
		}
		return b
	}, func() {
		// do nothing
	})
}

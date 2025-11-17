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

package file

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func TestRegister(t *testing.T) {
	modules.RegisterFileRollHook("mock", func() modules.RollHook {
		return &MockRollingHook{}
	})
	_, ok := modules.GetFileRollHook("mock")
	assert.True(t, ok)
	_, ok = modules.GetFileRollHook("none")
	assert.False(t, ok)
}

func TestProvision(t *testing.T) {
	modules.RegisterFileRollHook("mock", func() modules.RollHook {
		return &MockRollingHook{}
	})
	tests := []struct {
		name string
		c    *sinkConf
		err  string
		p    map[string]interface{}
	}{
		{
			name: "new props",
			c: &sinkConf{
				CheckInterval:      cast.DurationConf(500 * time.Millisecond),
				Path:               "test",
				FileType:           CSV_TYPE,
				Format:             message.FormatDelimited,
				Delimiter:          ",",
				RollingCount:       1000000,
				RollingNamePattern: "none",
				RollingHook:        "mock",
				RollingHookProps: map[string]any{
					"custom": "test",
				},
			},
			p: map[string]interface{}{
				"checkInterval":      500,
				"path":               "test",
				"fileType":           "csv",
				"format":             message.FormatDelimited,
				"rollingNamePattern": "none",
				"rollingHook":        "mock",
				"rollingHookProps": map[string]any{
					"custom": "test",
				},
			},
		},
		{
			name: "interval duration",
			c: &sinkConf{
				CheckInterval:      cast.DurationConf(10 * time.Second),
				Path:               "test",
				FileType:           CSV_TYPE,
				Format:             message.FormatDelimited,
				Delimiter:          ",",
				RollingCount:       1000000,
				RollingNamePattern: "none",
				RollingInterval:    cast.DurationConf(10 * time.Second),
			},
			p: map[string]interface{}{
				"path":               "test",
				"fileType":           "csv",
				"format":             message.FormatDelimited,
				"rollingNamePattern": "none",
				"rollingInterval":    "10s",
			},
		},
		{ // invalid rolling hook
			name: "invalid rolling hook",
			p: map[string]interface{}{
				"rollingInterval": 500,
				"rollingCount":    0,
				"rollingHook":     "invalid",
			},
			err: "rolling hook invalid is not registered",
		},
		{
			name: "invalid rolling props",
			err:  "missing props custom",
			p: map[string]interface{}{
				"rollingInterval": 500,
				"rollingCount":    0,
				"rollingHook":     "mock",
				"rollingHookProps": map[string]any{
					"delay": 25,
				},
			},
		},
	}
	ctx := mockContext.NewMockContext("test1", "test")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &fileSink{}
			err := m.Provision(ctx, tt.p)
			if tt.err != "" {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.c, m.c)
			}
		})
	}
}

func TestCollectRolling(t *testing.T) {
	// Remove existing files
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".log" {
			fmt.Println("Deleting file:", path)
			return os.Remove(path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	conf.IsTesting = true
	tests := []struct {
		name     string
		fname    string
		contents [2][]byte
	}{
		{
			name:  "lines",
			fname: "test_lines.log",
			contents: [2][]byte{
				[]byte("{\"key\":\"value0\",\"ts\":460}\n{\"key\":\"value1\",\"ts\":910}\n{\"key\":\"value2\",\"ts\":1360}"),
				[]byte("{\"key\":\"value3\",\"ts\":1810}\n{\"key\":\"value4\",\"ts\":2260}"),
			},
		},
	}

	// Create a stream context for testing
	ctx := mockContext.NewMockContext("rule", "testRolling")
	hook := &MockRollingHook{}
	modules.RegisterFileRollHook("mock", func() modules.RollHook {
		return hook
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a file sink with the temporary file path
			sink := &fileSink{}
			err := sink.Provision(ctx, map[string]interface{}{
				"path":               tt.fname,
				"fileType":           LINES_TYPE,
				"rollingInterval":    1000,
				"checkInterval":      500,
				"rollingCount":       0,
				"rollingNamePattern": "suffix",
				"rollingHook":        "mock",
				"rollingHookProps": map[string]any{
					"custom": 123,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			mockclock.ResetClock(10)
			err = sink.Connect(ctx, func(status string, message string) {
				// do nothing
			})
			if err != nil {
				t.Fatal(err)
			}
			c := mockclock.GetMockClock()

			for i := 0; i < 5; i++ {
				c.Add(450 * time.Millisecond)
				m := map[string]interface{}{"key": "value" + strconv.Itoa(i), "ts": c.Now().UnixMilli()}
				b, err := json.Marshal(m)
				assert.NoError(t, err)
				if err := sink.Collect(ctx, &xsql.RawTuple{Rawdata: b}); err != nil {
					t.Errorf("unexpected error: %s", err)
				}
			}
			c.After(2000 * time.Millisecond)
			if err = sink.Close(ctx); err != nil {
				t.Errorf("unexpected close error: %s", err)
			}
			// Should write to 2 files
			for i := 0; i < 2; i++ {
				// Read the contents of the temporary file and check if they match the collected items
				fn := fmt.Sprintf("test_%s-%d.log", LINES_TYPE, 460+1350*i)

				var contents []byte
				contents, err := os.ReadFile(fn)
				if err != nil {
					t.Fatal(err)
				}
				assert.Equal(t, contents, tt.contents[i])
			}
			exp := []string{"test_lines-460.log", "test_lines-1810.log"}
			assert.Equal(t, hook.result, exp)
		})
	}
}

type MockRollingHook struct {
	result []string
}

func (m *MockRollingHook) Provision(ctx api.StreamContext, props map[string]any) error {
	if _, ok := props["custom"]; !ok {
		return fmt.Errorf("missing props custom")
	}
	return nil
}

func (m *MockRollingHook) RollDone(ctx api.StreamContext, filePath string) error {
	ctx.GetLogger().Infof("roll done for %s", filePath)
	m.result = append(m.result, filePath)
	return nil
}

func (m *MockRollingHook) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("roll hook close")
	return nil
}

var _ modules.RollHook = &MockRollingHook{}

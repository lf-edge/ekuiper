// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/message"
	"os"
	"reflect"
	"testing"
)

// Unit test for Configure function
func TestConfigure(t *testing.T) {
	props := map[string]interface{}{
		"interval": 500,
		"path":     "test",
	}
	m := &fileSink{}
	err := m.Configure(props)
	if err != nil {
		t.Errorf("Configure() error = %v, wantErr nil", err)
	}
	if m.c.Interval != 500 {
		t.Errorf("Configure() Interval = %v, want 500", m.c.Interval)
	}
	if m.c.Path != "test" {
		t.Errorf("Configure() Path = %v, want test", m.c.Path)
	}
	err = m.Configure(map[string]interface{}{"interval": -1, "path": "test"})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Configure(map[string]interface{}{"interval": 500, "path": ""})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Configure(map[string]interface{}{"fileType": "csv2"})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Configure(map[string]interface{}{"interval": 500,
		"path":     "test",
		"fileType": "csv"})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
}

func TestFileSink_Configure(t *testing.T) {
	tests := []struct {
		name string
		c    *sinkConf
		p    map[string]interface{}
	}{
		{
			name: "default configurations",
			c: &sinkConf{
				Interval: 1000,
				Path:     "cache",
				FileType: LINES_TYPE,
			},
			p: map[string]interface{}{},
		},
		{
			name: "previous setting",
			c: &sinkConf{
				Interval: 500,
				Path:     "test",
				FileType: LINES_TYPE,
			},

			p: map[string]interface{}{
				"interval": 500,
				"path":     "test",
			},
		},
		{
			name: "new props",
			c: &sinkConf{
				Interval:  500,
				Path:      "test",
				FileType:  CSV_TYPE,
				Format:    message.FormatDelimited,
				Delimiter: ",",
			},
			p: map[string]interface{}{
				"interval": 500,
				"path":     "test",
				"fileType": "csv",
				"format":   message.FormatDelimited,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &fileSink{}
			if err := m.Configure(tt.p); err != nil {
				t.Errorf("fileSink.Configure() error = %v", err)
				return
			}
			if !reflect.DeepEqual(m.c, tt.c) {
				t.Errorf("fileSink.Configure() = %v, want %v", m.c, tt.c)
			}
		})
	}
}

func TestFileSink_Collect(t *testing.T) {
	tests := []struct {
		name    string
		ft      FileType
		fname   string
		content []byte
	}{
		{
			name:    "lines",
			ft:      LINES_TYPE,
			fname:   "test_lines",
			content: []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
		}, {
			name:    "json",
			ft:      JSON_TYPE,
			fname:   "test_json",
			content: []byte(`[{"key":"value1"}{"key":"value2"}]`),
		}, {
			name:    "csv",
			ft:      CSV_TYPE,
			fname:   "test_csv",
			content: []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
		},
	}

	// Create a stream context for testing
	contextLogger := conf.Log.WithField("rule", "test2")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	tf, _ := transform.GenTransform("", "json", "", "")
	vCtx := context.WithValue(ctx, context.TransKey, tf)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary file for testing
			tmpfile, err := os.CreateTemp("", tt.fname)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())
			// Create a file sink with the temporary file path
			sink := &fileSink{c: &sinkConf{Path: tmpfile.Name(), FileType: tt.ft, HasHeader: true}}
			sink.Open(ctx)

			// Test collecting a map item
			m := map[string]interface{}{"key": "value1"}
			if err := sink.Collect(vCtx, m); err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			// Test collecting another map item
			m = map[string]interface{}{"key": "value2"}
			if err := sink.Collect(ctx, m); err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if err = sink.Close(ctx); err != nil {
				t.Errorf("unexpected close error: %s", err)
			}
			// Read the contents of the temporary file and check if they match the collected items
			contents, err := os.ReadFile(tmpfile.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(contents, tt.content) {
				t.Errorf("expected %q but got %q", tt.content, string(contents))
			}
		})
	}
}

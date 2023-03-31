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
	"os"
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
}

func TestFileSink_Configure(t *testing.T) {
	tests := []struct {
		name    string
		c       *sinkConf
		p       map[string]interface{}
		wantErr bool
	}{
		{
			name: "valid configuration",
			c: &sinkConf{
				Interval: 1000,
				Path:     "cache",
			},
			p: map[string]interface{}{
				"interval": 500,
				"path":     "test",
			},
			wantErr: false,
		},
		{
			name: "invalid interval",

			c: &sinkConf{
				Interval: 1000,
				Path:     "cache",
			},

			p: map[string]interface{}{
				"interval": -500,
				"path":     "test",
			},
			wantErr: true,
		},
		{
			name: "empty path",

			c: &sinkConf{
				Interval: 1000,
				Path:     "cache",
			},

			p: map[string]interface{}{
				"interval": 500,
				"path":     "",
			},

			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &fileSink{}
			if err := m.Configure(tt.p); (err != nil) != tt.wantErr {
				t.Errorf("fileSink.Configure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileSink_Collect(t *testing.T) {
	// Create a temporary file for testing
	tmpfile, err := os.CreateTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	// Create a file sink with the temporary file path
	sink := &fileSink{c: &sinkConf{Path: tmpfile.Name()}}

	// Create a stream context for testing
	contextLogger := conf.Log.WithField("rule", "test2")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	tf, _ := transform.GenTransform("", "json", "", "")
	vCtx := context.WithValue(ctx, context.TransKey, tf)
	sink.Open(ctx)

	// Test collecting a string item
	str := "test string"
	if err := sink.Collect(vCtx, str); err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	// Test collecting a map item
	m := map[string]interface{}{"key": "value"}
	if err := sink.Collect(ctx, m); err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	// Test collecting an invalid item
	invalid := make(chan int)
	if err := sink.Collect(ctx, invalid); err == nil {
		t.Error("expected error but got nil")
	}

	// Close the file sink
	if err := sink.Close(ctx); err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	// Read the contents of the temporary file and check if they match the collected items
	contents, err := os.ReadFile(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	expected := "\"test string\"\n{\"key\":\"value\"}\n"
	if string(contents) != expected {
		t.Errorf("expected %q but got %q", expected, string(contents))
	}
}

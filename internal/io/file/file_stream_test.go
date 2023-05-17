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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/benbjohnson/clock"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/message"
)

func TestFileSinkCompress_Collect(t *testing.T) {
	tests := []struct {
		name     string
		ft       FileType
		fname    string
		content  []byte
		compress string
	}{
		{
			name:    "lines",
			ft:      LINES_TYPE,
			fname:   "test_lines",
			content: []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
		},
		{
			name:    "json",
			ft:      JSON_TYPE,
			fname:   "test_json",
			content: []byte(`[{"key":"value1"},{"key":"value2"}]`),
		},

		{
			name:     "lines",
			ft:       LINES_TYPE,
			fname:    "test_lines",
			content:  []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: GZIP,
		},

		{
			name:     "json",
			ft:       JSON_TYPE,
			fname:    "test_json",
			content:  []byte(`[{"key":"value1"},{"key":"value2"}]`),
			compress: GZIP,
		},

		{
			name:     "lines",
			ft:       LINES_TYPE,
			fname:    "test_lines",
			content:  []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: ZSTD,
		},

		{
			name:     "json",
			ft:       JSON_TYPE,
			fname:    "test_json",
			content:  []byte(`[{"key":"value1"},{"key":"value2"}]`),
			compress: ZSTD,
		},
	}

	// Create a stream context for testing
	contextLogger := conf.Log.WithField("rule", "test2")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	tf, _ := transform.GenTransform("", "json", "", "", "", []string{})
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
			sink := &fileSink{}
			f := message.FormatJson
			if tt.ft == CSV_TYPE {
				f = message.FormatDelimited
			}
			err = sink.Configure(map[string]interface{}{
				"path":               tmpfile.Name(),
				"fileType":           tt.ft,
				"hasHeader":          true,
				"format":             f,
				"rollingNamePattern": "none",
				"compression":        tt.compress,
			})
			if err != nil {
				t.Fatal(err)
			}
			err = sink.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}

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
			contents, err := os.ReadFile(tmpfile.Name())
			if err != nil {
				t.Fatal(err)
			}
			if tt.compress != "" {
				decompressor, _ := compressor.GetDecompressor(tt.compress)
				decompress, err := decompressor.Decompress(contents)
				if err != nil {
					t.Errorf("%v", err)
				}

				if !reflect.DeepEqual(decompress, tt.content) {
					t.Errorf("\nexpected\t %q \nbut got\t\t %q", tt.content, string(contents))
				}
			} else {
				if !reflect.DeepEqual(contents, tt.content) {
					t.Errorf("\nexpected\t %q \nbut got\t\t %q", tt.content, string(contents))
				}
			}

			// Read the contents of the temporary file and check if they match the collected items
			r := &FileSource{}
			dir := filepath.Dir(tmpfile.Name())
			filename := filepath.Base(tmpfile.Name())
			p := map[string]interface{}{
				"path":          filepath.Join(dir),
				"decompression": tt.compress,
				"fileType":      tt.ft,
			}

			err = r.Configure(filename, p)
			if err != nil {
				t.Errorf(err.Error())
				return
			}
			meta := map[string]interface{}{
				"file": filepath.Join(dir, filename),
			}
			mc := conf.Clock.(*clock.Mock)
			exp := []api.SourceTuple{
				api.NewDefaultSourceTupleWithTime(map[string]interface{}{"key": "value1"}, meta, mc.Now()),
				api.NewDefaultSourceTupleWithTime(map[string]interface{}{"key": "value2"}, meta, mc.Now()),
			}
			mock.TestSourceOpen(r, exp, t)
		})
	}
}

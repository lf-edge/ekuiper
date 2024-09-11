// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/v2/internal/compressor"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
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
			err = sink.Provision(ctx, map[string]interface{}{
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
			err = sink.Connect(ctx, func(status string, message string) {
				// do nothing
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := sink.Collect(ctx, &xsql.RawTuple{Rawdata: []byte("{\"key\":\"value1\"}")}); err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			// Test collecting another map item
			if err := sink.Collect(ctx, &xsql.RawTuple{Rawdata: []byte("{\"key\":\"value2\"}")}); err != nil {
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
			//r := &FileSource{}
			//dir := filepath.Dir(tmpfile.Name())
			//filename := filepath.Base(tmpfile.Name())
			//p := map[string]interface{}{
			//	"path":          filepath.Join(dir),
			//	"decompression": tt.compress,
			//	"fileType":      tt.ft,
			//}
			//
			//err = r.Configure(filename, p)
			//if err != nil {
			//	t.Errorf(err.Error())
			//	return
			//}
			//meta := map[string]interface{}{
			//	"file": filepath.Join(dir, filename),
			//}
			//mc := conf.Clock.(*clock.Mock)
			//exp := []api.SourceTuple{
			//	api.NewDefaultSourceTupleWithTime(map[string]interface{}{"key": "value1"}, meta, mc.Now()),
			//	api.NewDefaultSourceTupleWithTime(map[string]interface{}{"key": "value2"}, meta, mc.Now()),
			//}
			//mock.TestSourceOpen(r, exp, t)
		})
	}
}

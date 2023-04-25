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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"github.com/lf-edge/ekuiper/pkg/message"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Unit test for Configure function
func TestConfigure(t *testing.T) {
	props := map[string]interface{}{
		"interval": 500,
		"path":     "test",
	}
	m := File().(*fileSink)
	err := m.Configure(props)
	if err != nil {
		t.Errorf("Configure() error = %v, wantErr nil", err)
	}
	if *m.c.Interval != 500 {
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
	err = m.Configure(map[string]interface{}{"interval": 60, "path": "test", "checkInterval": -1})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Configure(map[string]interface{}{"rollingInterval": -1})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Configure(map[string]interface{}{"rollingCount": -1})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Configure(map[string]interface{}{"rollingCount": 0, "rollingInterval": 0})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Configure(map[string]interface{}{"RollingNamePattern": "test"})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Configure(map[string]interface{}{"RollingNamePattern": 0})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}

	for _, v := range []string{"none", "flate", "gzip", "zlib", ""} {
		err = m.Configure(map[string]interface{}{
			"interval":           500,
			"path":               "test",
			"compression":           v,
			"rollingNamePattern": "suffix",
		})
		if err != nil {
			t.Errorf("Configure() error = %v, wantErr nil", err)
		}
		if m.c.Compression != v {
			t.Errorf("Configure() Compression = %v, want %v", m.c.Compression, v)
		}
	}

	err = m.Configure(map[string]interface{}{
		"interval": 500,
		"path":     "test",
		"compression": "not_exist_algorithm",
	})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
}

func TestFileSink_Configure(t *testing.T) {
	var (
		defaultCheckInterval = (5 * time.Minute).Milliseconds()
		int500               = 500
		int64_500            = int64(int500)
	)

	tests := []struct {
		name string
		c    *sinkConf
		p    map[string]interface{}
	}{
		{
			name: "default configurations",
			c: &sinkConf{
				CheckInterval: &defaultCheckInterval,
				Path:          "cache",
				FileType:      LINES_TYPE,
				RollingCount:  1000000,
			},
			p: map[string]interface{}{},
		},
		{
			name: "previous setting",
			c: &sinkConf{
				Interval:      &int500,
				CheckInterval: &int64_500,
				Path:          "test",
				FileType:      LINES_TYPE,
				RollingCount:  1000000,
			},

			p: map[string]interface{}{
				"interval": 500,
				"path":     "test",
			},
		},
		{
			name: "new props",
			c: &sinkConf{
				CheckInterval:      &int64_500,
				Path:               "test",
				FileType:           CSV_TYPE,
				Format:             message.FormatDelimited,
				Delimiter:          ",",
				RollingCount:       1000000,
				RollingNamePattern: "none",
			},
			p: map[string]interface{}{
				"checkInterval":      500,
				"path":               "test",
				"fileType":           "csv",
				"format":             message.FormatDelimited,
				"rollingNamePattern": "none",
			},
		},
		{ // only set rolling interval
			name: "rolling",
			c: &sinkConf{
				CheckInterval:   &defaultCheckInterval,
				Path:            "cache",
				FileType:        LINES_TYPE,
				RollingInterval: 500,
				RollingCount:    0,
			},
			p: map[string]interface{}{
				"rollingInterval": 500,
				"rollingCount":    0,
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

// Test single file writing and flush by close
func TestFileSink_Collect(t *testing.T) {
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
		}, {
			name:    "json",
			ft:      JSON_TYPE,
			fname:   "test_json",
			content: []byte(`[{"key":"value1"},{"key":"value2"}]`),
		}, {
			name:    "csv",
			ft:      CSV_TYPE,
			fname:   "test_csv",
			content: []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
		},

		{
			name:    "lines",
			ft:      LINES_TYPE,
			fname:   "test_lines",
			content: []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: NONE_COMPRESS,
		}, {
			name:    "json",
			ft:      JSON_TYPE,
			fname:   "test_json",
			content: []byte(`[{"key":"value1"},{"key":"value2"}]`),
			compress: NONE_COMPRESS,
		}, {
			name:    "csv",
			ft:      CSV_TYPE,
			fname:   "test_csv",
			content: []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: NONE_COMPRESS,
		},

		{
			name:    "lines",
			ft:      LINES_TYPE,
			fname:   "test_lines",
			content: []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: GZIP,
		}, {
			name:    "json",
			ft:      JSON_TYPE,
			fname:   "test_json",
			content: []byte(`[{"key":"value1"},{"key":"value2"}]`),
			compress: GZIP,
		}, {
			name:    "csv",
			ft:      CSV_TYPE,
			fname:   "test_csv",
			content: []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: GZIP,
		},

		{
			name:    "lines",
			ft:      LINES_TYPE,
			fname:   "test_lines",
			content: []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: FLATE,
		}, {
			name:    "json",
			ft:      JSON_TYPE,
			fname:   "test_json",
			content: []byte(`[{"key":"value1"},{"key":"value2"}]`),
			compress: FLATE,
		}, {
			name:    "csv",
			ft:      CSV_TYPE,
			fname:   "test_csv",
			content: []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: FLATE,
		},

		{
			name:    "lines",
			ft:      LINES_TYPE,
			fname:   "test_lines",
			content: []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: ZLIB,
		}, {
			name:    "json",
			ft:      JSON_TYPE,
			fname:   "test_json",
			content: []byte(`[{"key":"value1"},{"key":"value2"}]`),
			compress: ZLIB,
		}, {
			name:    "csv",
			ft:      CSV_TYPE,
			fname:   "test_csv",
			content: []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: ZLIB,
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
				"compression":           tt.compress,
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
			// Read the contents of the temporary file and check if they match the collected items
			contents, err := os.ReadFile(tmpfile.Name())
			if err != nil {
				t.Fatal(err)
			}
			if tt.compress != "" && tt.compress != NONE_COMPRESS {
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

		})
	}
}

// Test file rolling by time
func TestFileSinkRolling_Collect(t *testing.T) {
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
		ft       FileType
		fname    string
		contents [2][]byte
		compress string
	}{
		{
			name:  "lines",
			ft:    LINES_TYPE,
			fname: "test_lines.log",
			contents: [2][]byte{
				[]byte("{\"key\":\"value0\",\"ts\":460}\n{\"key\":\"value1\",\"ts\":910}\n{\"key\":\"value2\",\"ts\":1360}"),
				[]byte("{\"key\":\"value3\",\"ts\":1810}\n{\"key\":\"value4\",\"ts\":2260}"),
			},
		}, {
			name:  "json",
			ft:    JSON_TYPE,
			fname: "test_json.log",
			contents: [2][]byte{
				[]byte("[{\"key\":\"value0\",\"ts\":460},{\"key\":\"value1\",\"ts\":910},{\"key\":\"value2\",\"ts\":1360}]"),
				[]byte("[{\"key\":\"value3\",\"ts\":1810},{\"key\":\"value4\",\"ts\":2260}]"),
			},
		},

		{
			name:  "lines",
			ft:    LINES_TYPE,
			fname: "test_lines_none.log",
			contents: [2][]byte{
				[]byte("{\"key\":\"value0\",\"ts\":460}\n{\"key\":\"value1\",\"ts\":910}\n{\"key\":\"value2\",\"ts\":1360}"),
				[]byte("{\"key\":\"value3\",\"ts\":1810}\n{\"key\":\"value4\",\"ts\":2260}"),
			},
			compress: NONE_COMPRESS,
		}, {
			name:  "json",
			ft:    JSON_TYPE,
			fname: "test_json_none.log",
			contents: [2][]byte{
				[]byte("[{\"key\":\"value0\",\"ts\":460},{\"key\":\"value1\",\"ts\":910},{\"key\":\"value2\",\"ts\":1360}]"),
				[]byte("[{\"key\":\"value3\",\"ts\":1810},{\"key\":\"value4\",\"ts\":2260}]"),
			},
			compress: NONE_COMPRESS,
		},

		{
			name:  "lines",
			ft:    LINES_TYPE,
			fname: "test_lines_gzip.log",
			contents: [2][]byte{
				[]byte("{\"key\":\"value0\",\"ts\":460}\n{\"key\":\"value1\",\"ts\":910}\n{\"key\":\"value2\",\"ts\":1360}"),
				[]byte("{\"key\":\"value3\",\"ts\":1810}\n{\"key\":\"value4\",\"ts\":2260}"),
			},
			compress: GZIP,
		}, {
			name:  "json",
			ft:    JSON_TYPE,
			fname: "test_json_gzip.log",
			contents: [2][]byte{
				[]byte("[{\"key\":\"value0\",\"ts\":460},{\"key\":\"value1\",\"ts\":910},{\"key\":\"value2\",\"ts\":1360}]"),
				[]byte("[{\"key\":\"value3\",\"ts\":1810},{\"key\":\"value4\",\"ts\":2260}]"),
			},
			compress: GZIP,
		},

		{
			name:  "lines",
			ft:    LINES_TYPE,
			fname: "test_lines_flate.log",
			contents: [2][]byte{
				[]byte("{\"key\":\"value0\",\"ts\":460}\n{\"key\":\"value1\",\"ts\":910}\n{\"key\":\"value2\",\"ts\":1360}"),
				[]byte("{\"key\":\"value3\",\"ts\":1810}\n{\"key\":\"value4\",\"ts\":2260}"),
			},
			compress: FLATE,
		}, {
			name:  "json",
			ft:    JSON_TYPE,
			fname: "test_json_flate.log",
			contents: [2][]byte{
				[]byte("[{\"key\":\"value0\",\"ts\":460},{\"key\":\"value1\",\"ts\":910},{\"key\":\"value2\",\"ts\":1360}]"),
				[]byte("[{\"key\":\"value3\",\"ts\":1810},{\"key\":\"value4\",\"ts\":2260}]"),
			},
			compress: FLATE,
		},

		{
			name:  "lines",
			ft:    LINES_TYPE,
			fname: "test_lines_zlib.log",
			contents: [2][]byte{
				[]byte("{\"key\":\"value0\",\"ts\":460}\n{\"key\":\"value1\",\"ts\":910}\n{\"key\":\"value2\",\"ts\":1360}"),
				[]byte("{\"key\":\"value3\",\"ts\":1810}\n{\"key\":\"value4\",\"ts\":2260}"),
			},
			compress: ZLIB,
		}, {
			name:  "json",
			ft:    JSON_TYPE,
			fname: "test_json_zlib.log",
			contents: [2][]byte{
				[]byte("[{\"key\":\"value0\",\"ts\":460},{\"key\":\"value1\",\"ts\":910},{\"key\":\"value2\",\"ts\":1360}]"),
				[]byte("[{\"key\":\"value3\",\"ts\":1810},{\"key\":\"value4\",\"ts\":2260}]"),
			},
			compress: ZLIB,
		},
	}

	// Create a stream context for testing
	contextLogger := conf.Log.WithField("rule", "testRolling")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	tf, _ := transform.GenTransform("", "json", "", "")
	vCtx := context.WithValue(ctx, context.TransKey, tf)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a file sink with the temporary file path
			sink := &fileSink{}
			err := sink.Configure(map[string]interface{}{
				"path":               tt.fname,
				"fileType":           tt.ft,
				"rollingInterval":    1000,
				"checkInterval":      500,
				"rollingCount":       0,
				"rollingNamePattern": "suffix",
				"compression":           tt.compress,
			})
			if err != nil {
				t.Fatal(err)
			}
			mockclock.ResetClock(10)
			err = sink.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			c := mockclock.GetMockClock()

			for i := 0; i < 5; i++ {
				c.Add(450 * time.Millisecond)
				m := map[string]interface{}{"key": "value" + strconv.Itoa(i), "ts": c.Now().UnixMilli()}
				if err := sink.Collect(vCtx, m); err != nil {
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
				var fn string
				if tt.compress != "" {
					fn = fmt.Sprintf("test_%s_%s-%d.log", tt.ft, tt.compress, 460+1350*i)
				} else {
					fn = fmt.Sprintf("test_%s-%d.log", tt.ft, 460+1350*i)
				}

				var contents []byte
				contents, err := os.ReadFile(fn)
				if err != nil {
					t.Fatal(err)
				}
				if tt.compress != "" && tt.compress != NONE_COMPRESS {
					decompressor, _ := compressor.GetDecompressor(tt.compress)
					contents, err = decompressor.Decompress(contents)
					if err != nil {
						t.Errorf("%v", err)
					}
				}
				if !reflect.DeepEqual(contents, tt.contents[i]) {
					t.Errorf("\nexpected\t %q \nbut got\t\t %q", tt.contents[i], string(contents))
				}
			}
		})
	}
}

// Test file rolling by count
func TestFileSinkRollingCount_Collect(t *testing.T) {
	// Remove existing files
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".dd" {
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
		ft       FileType
		fname    string
		contents [3][]byte
		compress string
	}{
		{
			name:  "csv",
			ft:    CSV_TYPE,
			fname: "test_csv_{{.ts}}.dd",
			contents: [3][]byte{
				[]byte("key,ts\nvalue0,460"),
				[]byte("key,ts\nvalue1,910"),
				[]byte("key,ts\nvalue2,1360"),
			},
		},

		{
			name:  "csv",
			ft:    CSV_TYPE,
			fname: "test_csv_none_{{.ts}}.dd",
			contents: [3][]byte{
				[]byte("key,ts\nvalue0,460"),
				[]byte("key,ts\nvalue1,910"),
				[]byte("key,ts\nvalue2,1360"),
			},
			compress: NONE_COMPRESS,
		},

		{
			name:  "csv",
			ft:    CSV_TYPE,
			fname: "test_csv_gzip_{{.ts}}.dd",
			contents: [3][]byte{
				[]byte("key,ts\nvalue0,460"),
				[]byte("key,ts\nvalue1,910"),
				[]byte("key,ts\nvalue2,1360"),
			},
			compress: GZIP,
		},

		{
			name:  "csv",
			ft:    CSV_TYPE,
			fname: "test_csv_zlib_{{.ts}}.dd",
			contents: [3][]byte{
				[]byte("key,ts\nvalue0,460"),
				[]byte("key,ts\nvalue1,910"),
				[]byte("key,ts\nvalue2,1360"),
			},
			compress: ZLIB,
		},

		{
			name:  "csv",
			ft:    CSV_TYPE,
			fname: "test_csv_flate_{{.ts}}.dd",
			contents: [3][]byte{
				[]byte("key,ts\nvalue0,460"),
				[]byte("key,ts\nvalue1,910"),
				[]byte("key,ts\nvalue2,1360"),
			},
			compress: FLATE,
		},
	}
	// Create a stream context for testing
	contextLogger := conf.Log.WithField("rule", "testRollingCount")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	tf, _ := transform.GenTransform("", "delimited", "", ",")
	vCtx := context.WithValue(ctx, context.TransKey, tf)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a file sink with the temporary file path
			sink := &fileSink{}
			err := sink.Configure(map[string]interface{}{
				"path":               tt.fname,
				"fileType":           tt.ft,
				"rollingInterval":    0,
				"rollingCount":       1,
				"rollingNamePattern": "none",
				"hasHeader":          true,
				"format":             "delimited",
				"compression":           tt.compress,
			})
			if err != nil {
				t.Fatal(err)
			}
			mockclock.ResetClock(10)
			err = sink.Open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			c := mockclock.GetMockClock()

			for i := 0; i < 3; i++ {
				c.Add(450 * time.Millisecond)
				m := map[string]interface{}{"key": "value" + strconv.Itoa(i), "ts": c.Now().UnixMilli()}
				if err := sink.Collect(vCtx, m); err != nil {
					t.Errorf("unexpected error: %s", err)
				}
			}
			c.After(2000 * time.Millisecond)
			if err = sink.Close(ctx); err != nil {
				t.Errorf("unexpected close error: %s", err)
			}
			// Should write to 2 files
			for i := 0; i < 3; i++ {
				// Read the contents of the temporary file and check if they match the collected items
				var fn string
				if tt.compress != "" {
					fn = fmt.Sprintf("test_%s_%s_%d.dd", tt.ft, tt.compress, 460+450*i)
				} else {
					fn = fmt.Sprintf("test_%s_%d.dd", tt.ft, 460+450*i)
				}

				contents, err := os.ReadFile(fn)
				if err != nil {
					t.Fatal(err)
				}
				if tt.compress != "" && tt.compress != NONE_COMPRESS {
					decompressor, _ := compressor.GetDecompressor(tt.compress)
					contents, err = decompressor.Decompress(contents)
					if err != nil {
						t.Errorf("%v", err)
					}
				}
				if !reflect.DeepEqual(contents, tt.contents[i]) {
					t.Errorf("\nexpected\t %q \nbut got\t\t %q", tt.contents[i], string(contents))
				}
			}
		})
	}
}



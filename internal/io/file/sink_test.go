// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/compressor"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// Unit test for Configure function
func TestConfigure(t *testing.T) {
	ctx := mockContext.NewMockContext("test1", "test")
	props := map[string]interface{}{
		"interval": 500,
		"path":     "test",
	}
	m := &fileSink{}
	err := m.Provision(ctx, props)
	if err != nil {
		t.Errorf("Configure() error = %v, wantErr nil", err)
	}
	if m.c.Path != "test" {
		t.Errorf("Configure() Path = %v, want test", m.c.Path)
	}
	err = m.Provision(ctx, map[string]interface{}{"interval": 500, "path": ""})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Provision(ctx, map[string]interface{}{"fileType": "csv2"})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Provision(ctx, map[string]interface{}{
		"interval": 500,
		"path":     "test",
		"fileType": "csv",
	})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Provision(ctx, map[string]interface{}{"interval": 60, "path": "test", "checkInterval": -1})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Provision(ctx, map[string]interface{}{"rollingInterval": -1})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Provision(ctx, map[string]interface{}{"rollingCount": -1})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Provision(ctx, map[string]interface{}{"rollingCount": 0, "rollingInterval": 0})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Provision(ctx, map[string]interface{}{"RollingNamePattern": "test"})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
	err = m.Provision(ctx, map[string]interface{}{"RollingNamePattern": 0})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}

	for k := range compressionTypes {
		err = m.Provision(ctx, map[string]interface{}{
			"interval":           500,
			"path":               "test",
			"compression":        k,
			"rollingNamePattern": "suffix",
		})
		if err != nil {
			t.Errorf("Configure() error = %v, wantErr nil", err)
		}
		if m.c.Compression != k {
			t.Errorf("Configure() Compression = %v, want %v", m.c.Compression, k)
		}
	}

	err = m.Provision(ctx, map[string]interface{}{
		"interval":           500,
		"path":               "test",
		"compression":        "",
		"rollingNamePattern": "suffix",
	})
	if err != nil {
		t.Errorf("Configure() error = %v, wantErr nil", err)
	}
	if m.c.Compression != "" {
		t.Errorf("Configure() Compression = %v, want %v", m.c.Compression, "")
	}

	err = m.Provision(ctx, map[string]interface{}{
		"interval":    500,
		"path":        "test",
		"compression": "not_exist_algorithm",
	})
	if err == nil {
		t.Errorf("Configure() error = %v, wantErr not nil", err)
	}
}

func TestFileSink_Configure(t *testing.T) {
	defaultCheckInterval := 5 * time.Minute

	tests := []struct {
		name string
		c    *sinkConf
		p    map[string]interface{}
	}{
		{
			name: "default configurations",
			c: &sinkConf{
				CheckInterval: cast.DurationConf(defaultCheckInterval),
				Path:          "cache",
				FileType:      LINES_TYPE,
				RollingCount:  1000000,
			},
			p: map[string]interface{}{},
		},
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
			},
			p: map[string]interface{}{
				"checkInterval":      "500ms",
				"path":               "test",
				"fileType":           "csv",
				"format":             message.FormatDelimited,
				"rollingNamePattern": "none",
			},
		},
		{ // only set rolling interval
			name: "rolling",
			c: &sinkConf{
				CheckInterval:   cast.DurationConf(500 * time.Millisecond),
				Path:            "cache",
				FileType:        LINES_TYPE,
				RollingInterval: cast.DurationConf(500 * time.Millisecond),
				RollingCount:    0,
			},
			p: map[string]interface{}{
				"rollingInterval": 500,
				"rollingCount":    0,
			},
		},
		{
			name: "fields",
			c: &sinkConf{
				CheckInterval:   cast.DurationConf(5 * time.Second),
				Path:            "cache",
				FileType:        LINES_TYPE,
				RollingInterval: cast.DurationConf(5 * time.Second),
				RollingCount:    0,
				Fields:          []string{"c", "a", "b"},
			},
			p: map[string]interface{}{
				"rollingInterval": "5s",
				"rollingCount":    0,
				"fields":          []string{"c", "a", "b"},
			},
		},
		{ // only set rolling size
			name: "rollingSize",
			c: &sinkConf{
				CheckInterval: cast.DurationConf(defaultCheckInterval),
				Path:          "cache",
				FileType:      LINES_TYPE,
				RollingSize:   1024,
				RollingCount:  0,
			},
			p: map[string]interface{}{
				"rollingSize":  1024,
				"rollingCount": 0,
			},
		},
	}
	ctx := mockContext.NewMockContext("test1", "test")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &fileSink{}
			if err := m.Provision(ctx, tt.p); err != nil {
				t.Errorf("fileSink.Configure() error = %v", err)
				return
			}
			assert.Equal(t, tt.c, m.c)
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
		},
		{
			name:    "json",
			ft:      JSON_TYPE,
			fname:   "test_json",
			content: []byte(`[{"key":"value1"},{"key":"value2"}]`),
		},
		{
			name:    "csv",
			ft:      CSV_TYPE,
			fname:   "test_csv",
			content: []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
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
			name:     "csv",
			ft:       CSV_TYPE,
			fname:    "test_csv",
			content:  []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
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
		{
			name:     "csv",
			ft:       CSV_TYPE,
			fname:    "test_csv",
			content:  []byte("key\n{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress: ZSTD,
		},
	}

	ctx := mockContext.NewMockContext("test1", "test")
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
				"fields":             []string{"key"},
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
			// Read the contents of the temporary file and check if they match the collected items
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
		})
	}
}

// Test single message with header defined
func TestCSVSingMessHeader(t *testing.T) {
	tests := []struct {
		name    string
		fname   string
		content []byte
	}{
		{
			name:    "csv",
			fname:   "test_csvh",
			content: []byte("id:name\n12:test\n11:value2"),
		},
	}

	ctx := mockContext.NewMockContext("test1", "test")
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
			f := message.FormatDelimited
			err = sink.Provision(ctx, map[string]interface{}{
				"path":               tmpfile.Name(),
				"fileType":           CSV_TYPE,
				"hasHeader":          true,
				"delimiter":          ":",
				"format":             f,
				"rollingNamePattern": "none",
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

			if err := sink.Collect(ctx, &xsql.RawTuple{Rawdata: []byte{0x3a, 0x0, 0x0, 0x0, 0x7, 0x69, 0x64, 0x3a, 0x6e, 0x61, 0x6d, 0x65, 0x31, 0x32, 0x3a, 0x74, 0x65, 0x73, 0x74}}); err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			// Test collecting another map item
			if err := sink.Collect(ctx, &xsql.RawTuple{Rawdata: []byte("11:value2")}); err != nil {
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
			assert.Equal(t, tt.content, contents)
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
		},
		{
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
			fname: "test_lines_gzip.log",
			contents: [2][]byte{
				[]byte("{\"key\":\"value0\",\"ts\":460}\n{\"key\":\"value1\",\"ts\":910}\n{\"key\":\"value2\",\"ts\":1360}"),
				[]byte("{\"key\":\"value3\",\"ts\":1810}\n{\"key\":\"value4\",\"ts\":2260}"),
			},
			compress: GZIP,
		},
		{
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
			fname: "test_lines_zstd.log",
			contents: [2][]byte{
				[]byte("{\"key\":\"value0\",\"ts\":460}\n{\"key\":\"value1\",\"ts\":910}\n{\"key\":\"value2\",\"ts\":1360}"),
				[]byte("{\"key\":\"value3\",\"ts\":1810}\n{\"key\":\"value4\",\"ts\":2260}"),
			},
			compress: ZSTD,
		},
		{
			name:  "json",
			ft:    JSON_TYPE,
			fname: "test_json_zstd.log",
			contents: [2][]byte{
				[]byte("[{\"key\":\"value0\",\"ts\":460},{\"key\":\"value1\",\"ts\":910},{\"key\":\"value2\",\"ts\":1360}]"),
				[]byte("[{\"key\":\"value3\",\"ts\":1810},{\"key\":\"value4\",\"ts\":2260}]"),
			},
			compress: ZSTD,
		},
	}

	// Create a stream context for testing
	ctx := mockContext.NewMockContext("rule", "testRolling")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a file sink with the temporary file path
			sink := &fileSink{}
			err := sink.Provision(ctx, map[string]interface{}{
				"path":               tt.fname,
				"fileType":           tt.ft,
				"rollingInterval":    1000,
				"checkInterval":      500,
				"rollingCount":       0,
				"rollingNamePattern": "suffix",
				"compression":        tt.compress,
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
				if tt.compress != "" {
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

func TestFileSinkReopen(t *testing.T) {
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
	assert.NoError(t, err)
	conf.IsTesting = true
	tmpfile, err := os.CreateTemp("", "reopen.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	// Create a stream context for testing
	ctx := mockContext.NewMockContext("test1", "test")

	sink := &fileSink{}
	err = sink.Provision(ctx, map[string]interface{}{
		"path":               tmpfile.Name(),
		"fileType":           LINES_TYPE,
		"format":             "json",
		"rollingNamePattern": "none",
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

	// Test collecting a map item
	if err := sink.Collect(ctx, &xsql.RawTuple{Rawdata: []byte("{\"key\":\"value1\"}")}); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	sink.Close(ctx)

	exp := []byte(`{"key":"value1"}`)
	contents, err := os.ReadFile(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(contents, exp) {
		t.Errorf("\nexpected\t %q \nbut got\t\t %q", string(exp), string(contents))
	}

	sink = &fileSink{}
	err = sink.Provision(ctx, map[string]interface{}{
		"path":               tmpfile.Name(),
		"fileType":           LINES_TYPE,
		"hasHeader":          true,
		"format":             "json",
		"rollingNamePattern": "none",
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
	// Test collecting another map item
	if err := sink.Collect(ctx, &xsql.RawTuple{Rawdata: []byte("{\"key\":\"value2\"}")}); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if err = sink.Close(ctx); err != nil {
		t.Errorf("unexpected close error: %s", err)
	}

	exp = []byte(`{"key":"value2"}`)
	contents, err = os.ReadFile(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(contents, exp) {
		t.Errorf("\nexpected\t %q \nbut got\t\t %q", string(exp), string(contents))
	}
}

// Test single file writing and flush by close
func TestFileCompressAndEncrypt(t *testing.T) {
	conf.InitConf()
	tests := []struct {
		name       string
		ft         FileType
		fname      string
		content    []byte
		compress   string
		encryption string
	}{
		{
			name:       "lines with encryption",
			ft:         LINES_TYPE,
			fname:      "test_lines",
			content:    []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			encryption: "aes",
		},
		{
			name:       "lines with compress and encryption",
			ft:         LINES_TYPE,
			fname:      "test_lines",
			content:    []byte("{\"key\":\"value1\"}\n{\"key\":\"value2\"}"),
			compress:   GZIP,
			encryption: "aes",
		},
	}

	// Create a stream context for testing
	ctx := mockContext.NewMockContext("rule1", "op1")
	_ = os.Mkdir("tmp", 0o777)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary file for testing
			tmpfile, err := os.CreateTemp("tmp", tt.fname)
			if err != nil {
				t.Fatal(err)
			}
			tmpfile.Close()
			defer os.Remove(tmpfile.Name())
			// Create a file sink with the temporary file path
			sink := &fileSink{}
			f := message.FormatJson
			err = sink.Provision(ctx, map[string]interface{}{
				"path":               tmpfile.Name(),
				"fileType":           tt.ft,
				"format":             f,
				"rollingNamePattern": "none",
				"compression":        tt.compress,
				"fields":             []string{"key"},
				"encryption":         tt.encryption,
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

			// Test collecting a map item
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
			// Read the contents of the temporary file and check if they match the collected items
			contents, err := os.ReadFile(tmpfile.Name())
			if err != nil {
				t.Fatal(err)
			}
			// Decrypt then uncompress
			revert := Decrypt(contents)
			// uncompress
			if tt.compress != "" {
				decompressor, _ := compressor.GetDecompressor(tt.compress)
				decompress, err := decompressor.Decompress(revert)
				if err != nil {
					t.Errorf("%v", err)
				}

				assert.Equal(t, decompress, tt.content)
			} else {
				assert.Equal(t, revert, tt.content)
			}
		})
	}
}

func Decrypt(contents []byte) []byte {
	key := conf.Config.AesKey
	// Create a new AES cipher block using the key
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	// Get IV from the encrypted data
	iv := contents[:aes.BlockSize]
	// Get the actual encrypted data
	secret := contents[aes.BlockSize:]
	// create a new CFB decrypter
	dstream := cipher.NewCFBDecrypter(block, iv) //nolint:staticcheck
	// decrypt the data
	decrypted := make([]byte, len(secret))
	dstream.XORKeyStream(decrypted, secret)
	return decrypted
}

// Test size-based rolling
func TestFileSinkRollingSize_Collect(t *testing.T) {
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
		name          string
		ft            FileType
		fname         string
		rollingSize   int64
		dataSize      int
		dataCount     int
		expectedFiles int
		compress      string
	}{
		{
			name:          "lines_size_rolling",
			ft:            LINES_TYPE,
			fname:         "test_size_lines.log",
			rollingSize:   100, // 100 bytes
			dataSize:      33,  // actual: {"index":N,"data":"test_value_N"} = 33 bytes
			dataCount:     10,  // 10 items: 33 + (1+33) + (1+33) = 101 bytes -> roll after 3 items
			expectedFiles: 4,   // Post-write check: 3+3+3+1 items = 4 files
		},
		{
			name:          "json_size_rolling",
			ft:            JSON_TYPE,
			fname:         "test_size_json.log",
			rollingSize:   100, // 100 bytes
			dataSize:      33,  // same data size
			dataCount:     10,  // JSON: "[" (1) + item (33) + "," (1) + item (33) + "," (1) + item (33) = 103
			expectedFiles: 4,   // Post-write check: 3+3+3+1 items = 4 files
		},
		{
			name:          "lines_size_rolling_gzip",
			ft:            LINES_TYPE,
			fname:         "test_size_lines_gzip.log",
			rollingSize:   100, // 100 bytes (before compression)
			dataSize:      33,
			dataCount:     10,
			expectedFiles: 4, // Same as lines without compression
			compress:      GZIP,
		},
		{
			name:          "json_size_rolling_zstd",
			ft:            JSON_TYPE,
			fname:         "test_size_json_zstd.log",
			rollingSize:   100,
			dataSize:      33,
			dataCount:     10,
			expectedFiles: 4, // Same as JSON without compression
			compress:      ZSTD,
		},
	}

	ctx := mockContext.NewMockContext("rule", "testRollingSize")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &fileSink{}
			err := sink.Provision(ctx, map[string]interface{}{
				"path":               tt.fname,
				"fileType":           tt.ft,
				"rollingSize":        tt.rollingSize,
				"rollingCount":       0,
				"rollingInterval":    0,
				"rollingNamePattern": "suffix",
				"compression":        tt.compress,
			})
			if err != nil {
				t.Fatal(err)
			}

			// Use mockclock to generate unique timestamps
			mockclock.ResetClock(100)
			err = sink.Connect(ctx, func(status string, message string) {})
			if err != nil {
				t.Fatal(err)
			}
			c := mockclock.GetMockClock()

			// Collect data
			for i := 0; i < tt.dataCount; i++ {
				// Advance clock to ensure unique timestamps for each roll
				c.Add(10 * time.Millisecond)
				data := fmt.Sprintf("{\"index\":%d,\"data\":\"test_value_%d\"}", i, i)
				if err := sink.Collect(ctx, &xsql.RawTuple{Rawdata: []byte(data)}); err != nil {
					t.Errorf("unexpected error: %s", err)
				}
			}

			if err = sink.Close(ctx); err != nil {
				t.Errorf("unexpected close error: %s", err)
			}

			// Check if the expected number of files were created
			files, err := filepath.Glob(fmt.Sprintf("test_size_%s*.log", tt.ft))
			if err != nil {
				t.Fatal(err)
			}

			if len(files) != tt.expectedFiles {
				t.Errorf("expected %d files, but got %d files: %v", tt.expectedFiles, len(files), files)
			}

			// Cleanup
			for _, f := range files {
				os.Remove(f)
			}
		})
	}
}

// Test provision validation for rolling size
func TestFileSink_ProvisionRollingSize(t *testing.T) {
	ctx := mockContext.NewMockContext("test1", "test")

	// Valid: only rollingSize set
	m := &fileSink{}
	err := m.Provision(ctx, map[string]interface{}{
		"path":        "test.log",
		"rollingSize": 1024,
	})
	if err != nil {
		t.Errorf("Provision with rollingSize should succeed, got error: %v", err)
	}
	if m.c.RollingSize != 1024 {
		t.Errorf("Expected RollingSize 1024, got %d", m.c.RollingSize)
	}

	// Invalid: no rolling condition set
	m2 := &fileSink{}
	err = m2.Provision(ctx, map[string]interface{}{
		"path":            "test.log",
		"rollingSize":     0,
		"rollingCount":    0,
		"rollingInterval": 0,
	})
	if err == nil {
		t.Error("Provision should fail when no rolling condition is set")
	}

	// Valid: rollingSize + rollingCount
	m3 := &fileSink{}
	err = m3.Provision(ctx, map[string]interface{}{
		"path":         "test.log",
		"rollingSize":  2048,
		"rollingCount": 100,
	})
	if err != nil {
		t.Errorf("Provision with multiple rolling conditions should succeed, got error: %v", err)
	}
	if m3.c.RollingSize != 2048 {
		t.Errorf("Expected RollingSize 2048, got %d", m3.c.RollingSize)
	}
	if m3.c.RollingCount != 100 {
		t.Errorf("Expected RollingCount 100, got %d", m3.c.RollingCount)
	}
}

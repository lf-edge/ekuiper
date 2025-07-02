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

package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestSourceProvision(t *testing.T) {
	// Create and write temp file
	tmpfile, err := os.CreateTemp("", "test.lines")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	if _, err = tmpfile.Write([]byte("content")); err != nil {
		t.Fatal(err)
	}
	if err = tmpfile.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Dir(tmpfile.Name())
	name := filepath.Base(tmpfile.Name())
	_, wrongMoveToErr := conf.GetLoc("ddd/dd")
	relPath, err := conf.GetLoc("data")
	if err != nil {
		t.Fatal(err)
	}
	ctx := mockContext.NewMockContext("testsourcep", "test")
	m := &Source{}
	tests := []struct {
		name  string
		props map[string]any
		c     *SourceConfig
		e     string
	}{
		{
			name: "normal",
			props: map[string]any{
				"datasource": name,
				"path":       path,
			},
			c: &SourceConfig{
				FileName: name,
				Path:     path,
				FileType: string(JSON_TYPE),
			},
		},
		{
			name: "invalid format",
			props: map[string]any{
				"datasource":   name,
				"path":         path,
				"sendInterval": "ddd",
			},
			e: fmt.Sprintf("read properties map[datasource:%s path:%s sendInterval:ddd] fail with error: 1 error(s) decoding:\n\n* error decoding 'sendInterval': time: invalid duration \"ddd\"", name, path),
		},
		{
			name: "missing file type",
			props: map[string]any{
				"datasource": name,
				"path":       path,
				"fileType":   "",
			},
			e: "missing or invalid property fileType, must be 'json'",
		},
		{
			name: "invalid reader prop",
			props: map[string]any{
				"datasource": name,
				"path":       path,
				"fileType":   "csv",
				"hasHeader":  "uvw",
			},
			e: "1 error(s) decoding:\n\n* 'hasHeader' expected type 'bool', got unconvertible type 'string', value: 'uvw'",
		},
		{
			name: "missing path",
			props: map[string]any{
				"datasource": name,
			},
			e: "missing property Path",
		},
		{
			name: "invalid path",
			props: map[string]any{
				"datasource": name,
				"path":       "dddd/ddd",
			},
			e: "invalid path dddd/ddd",
		},
		{
			name: "invalid file",
			props: map[string]any{
				"datasource": "notexist",
				"path":       path,
			},
			e: fmt.Sprintf("file %s not exist", filepath.Join(path, "notexist")),
		},
		{
			name: "dir and lines",
			props: map[string]any{
				"datasource":       "",
				"path":             "data",
				"ignoreStartLines": -2,
				"ignoreEndLines":   -2,
			},
			c: &SourceConfig{
				FileName:         "",
				Path:             relPath,
				FileType:         string(JSON_TYPE),
				IgnoreStartLines: 0,
				IgnoreEndLines:   0,
			},
		},
		{
			name: "wrong action after read",
			props: map[string]any{
				"datasource":      name,
				"path":            path,
				"actionAfterRead": 4,
			},
			e: "invalid actionAfterRead: 4",
		},
		{
			name: "missing move to",
			props: map[string]any{
				"datasource":      name,
				"path":            path,
				"actionAfterRead": 2,
			},
			e: "missing moveTo when actionAfterRead is 2",
		},
		{
			name: "wrong move to",
			props: map[string]any{
				"datasource":      name,
				"path":            path,
				"actionAfterRead": 2,
				"moveTo":          "ddd/dd",
			},
			e: fmt.Sprintf("invalid moveTo : %v", wrongMoveToErr),
		},
		{
			name: "move to dir",
			props: map[string]any{
				"datasource":      name,
				"path":            path,
				"actionAfterRead": 2,
				"moveTo":          filepath.Join(path, "ddd"),
			},
			c: &SourceConfig{
				FileName:        name,
				Path:            path,
				FileType:        string(JSON_TYPE),
				ActionAfterRead: 2,
				MoveTo:          filepath.Join(path, "ddd"),
			},
		},
		{
			name: "no decompression for stream types",
			props: map[string]any{
				"datasource":    name,
				"path":          path,
				"fileType":      LINES_TYPE,
				"decompression": "gzip",
			},
			e: "decompression is not supported for lines file type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := m.Provision(ctx, tt.props)
			if tt.e == "" {
				assert.NoError(t, err)
				assert.Equal(t, tt.c, m.config)
			} else {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.e)
			}
		})
	}
}

func TestLines(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(path, "test")
	func() {
		src, err := os.Open(filepath.Join(path, "test.lines"))
		assert.NoError(t, err)
		defer src.Close()
		dest, err := os.Create(filepath.Join(path, "test.lines.copy"))
		assert.NoError(t, err)
		defer dest.Close()
		_, err = io.Copy(dest, src)
		assert.NoError(t, err)
	}()

	meta := map[string]any{
		"file": filepath.Join(path, "test.lines.copy"),
	}
	mc := timex.Clock
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple([]byte("{\"id\": 1,\"name\": \"John Doe\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"id\": 2,\"name\": \"Jane Doe\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"id\": 3,\"name\": \"John Smith\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("[{\"id\": 4,\"name\": \"John Smith\"},{\"id\": 5,\"name\": \"John Smith\"}]"), meta, mc.Now()),
	}
	r := GetSource()
	mock.TestSourceConnector(t, r, map[string]any{
		"path":            path,
		"fileType":        "lines",
		"datasource":      "test.lines.copy",
		"actionAfterRead": 1,
	}, exp, func() {
		// do nothing
	})
}

func TestCSVBatch(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(path, "test")

	meta1 := map[string]any{
		"file": filepath.Join(path, "csv", "a.csv"),
	}
	meta2 := map[string]any{
		"file": filepath.Join(path, "csv", "b.csv"),
	}
	mc := timex.Clock
	exp := []api.MessageTuple{
		model.NewDefaultSourceTuple(map[string]any{"@": "#", "id": "1", "ts": "1670170500", "value": "161.927872"}, meta1, mc.Now()),
		model.NewDefaultSourceTuple(map[string]any{"@": "#", "id": "2", "ts": "1670170900", "value": "176"}, meta1, mc.Now()),
		model.NewDefaultSourceTuple(map[string]any{"id": "33", "ts": "1670270500", "humidity": "89"}, meta2, mc.Now()),
		model.NewDefaultSourceTuple(map[string]any{"id": "44", "ts": "1670270900", "humidity": "76"}, meta2, mc.Now()),
	}
	r := GetSource()
	mock.TestSourceConnector(t, r, map[string]any{
		"path":             path,
		"fileType":         "csv",
		"datasource":       "csv",
		"hasHeader":        true,
		"delimiter":        "\t",
		"ignoreStartLines": 3,
		"ignoreEndLines":   1,
	}, exp, func() {
		// do nothing
	})
}

func TestBatch(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(path, "test", "json")

	meta := map[string]any{
		"file": filepath.Join(path, "f1.json"),
	}
	mc := timex.Clock
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple([]byte("[{\"id\": 1,\"name\": \"John Doe\",\"height\": 1.82},{\"id\": 2,\"name\": \"Jane Doe\",\"height\": 1.65}]"), meta, mc.Now()),
	}
	r := GetSource()
	mock.TestSourceConnector(t, r, map[string]any{
		"path":       path,
		"fileType":   "json",
		"datasource": "f1.json",
	}, exp, func() {
		// do nothing
	})
}

func TestIgnoreLines(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test.ignore.lines")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	if _, err = tmpfile.Write([]byte("{\"id\": 1,\"name\": \"John Doe\"}\n{\"id\": 2,\"name\": \"Jane Doe\"}\n{\"id\": 3,\"name\": \"John Smith\"}\n[{\"id\": 4,\"name\": \"John Smith\"},{\"id\": 5,\"name\": \"John Smith\"}]")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Dir(tmpfile.Name())
	name := filepath.Base(tmpfile.Name())
	meta := map[string]any{
		"file": tmpfile.Name(),
	}
	mc := timex.Clock
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple([]byte("{\"id\": 2,\"name\": \"Jane Doe\"}"), meta, mc.Now()),
		model.NewDefaultRawTuple([]byte("{\"id\": 3,\"name\": \"John Smith\"}"), meta, mc.Now()),
	}
	r := GetSource()
	r.(api.Bounded).SetEofIngest(func(ctx api.StreamContext, _ string) {
		fmt.Printf("eof")
	})
	moved, err := os.MkdirTemp(path, "test")
	defer os.RemoveAll(moved)
	assert.NoError(t, err)
	mock.TestSourceConnector(t, r, map[string]any{
		"path":             path,
		"fileType":         "lines",
		"datasource":       name,
		"actionAfterRead":  2,
		"moveTo":           moved,
		"ignoreStartLines": 1,
		"ignoreEndLines":   1,
	}, exp, func() {
		// do nothing
	})
	// wait for file moved
	time.Sleep(100 * time.Millisecond)
	movedFile := filepath.Join(moved, name)
	_, err = os.Stat(movedFile)
	if os.IsNotExist(err) {
		assert.Fail(t, "copy file is not existed")
	} else {
		os.Remove(movedFile)
	}
}

func TestIntervalAndDir(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(path, "test")

	meta := map[string]any{
		"file": filepath.Join(path, "test.lines"),
	}
	exp := []api.MessageTuple{
		model.NewDefaultRawTupleIgnoreTs([]byte("{\"id\": 2,\"name\": \"Jane Doe\"}"), meta),
		model.NewDefaultRawTupleIgnoreTs([]byte("{\"id\": 3,\"name\": \"John Smith\"}"), meta),
		model.NewDefaultRawTupleIgnoreTs([]byte("[{\"id\": 4,\"name\": \"John Smith\"},{\"id\": 5,\"name\": \"John Smith\"}]"), meta),
	}
	r := GetSource()
	mock.TestSourceConnector(t, r, map[string]any{
		"path":             path,
		"fileType":         "lines",
		"interval":         "1s",
		"sendInterval":     "100ms",
		"ignoreStartLines": 1,
		// only for test
		"ignoreTs": true,
	}, exp, func() {
		for i := 0; i < 10; i++ {
			timex.Add(2 * time.Second)
			time.Sleep(100 * time.Millisecond)
		}
	})
}

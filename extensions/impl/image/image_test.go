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

package image

import (
	"bytes"
	"image/jpeg"
	"image/png"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestConfigure(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		c     *c
		err   string
	}{
		{
			name: "wrong type",
			props: map[string]any{
				"maxAge": "0.11",
			},
			err: "1 error(s) decoding:\n\n* 'maxAge' expected type 'int', got unconvertible type 'string', value: '0.11'",
		},
		{
			name: "missing path",
			props: map[string]any{
				"imageFormat": "jpeg",
			},
			err: "path is required",
		},
		{
			name: "wrong format",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "abc",
			},
			err: "invalid image format: abc",
		},
		{
			name: "default age",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
				"maxCount":    1,
			},
			c: &c{
				Path:        "data",
				ImageFormat: "png",
				MaxCount:    1,
				MaxAge:      72,
			},
		},
		{
			name: "default count",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
				"maxAge":      0.11,
			},
			c: &c{
				Path:        "data",
				ImageFormat: "png",
				MaxCount:    1000,
				MaxAge:      0,
			},
		},
		{
			name: "wrong max age",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
				"maxAge":      -1,
			},
			err: "invalid max age: -1",
		},
		{
			name: "wrong max count",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
				"maxCount":    -1,
			},
			err: "invalid max count: -1",
		},
	}
	s := &imageSink{}
	ctx := mockContext.NewMockContext("testConfigure", "op")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.Provision(ctx, test.props)
			if test.err == "" {
				assert.NoError(t, err)
				assert.Equal(t, test.c, s.c)
			} else {
				assert.EqualError(t, err, test.err)
			}
		})
	}
}

func TestSave(t *testing.T) {
	tests := []struct {
		name  string
		props map[string]any
		image string
		err   string
	}{
		{
			name: "normal",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
			},
			image: "../../../docs/en_US/wechat.png",
		},
		{
			name: "wrong format",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "jpeg",
			},
			image: "../../../docs/en_US/wechat.png",
			err:   "invalid JPEG format: missing SOI marker",
		},
		{
			name: "normal jpeg",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "jpeg",
			},
			image: "ekuiper.jpg",
		},
		{
			name: "wrong png",
			props: map[string]any{
				"path":        "data",
				"imageFormat": "png",
			},
			image: "ekuiper.jpg",
			err:   "png: invalid format: not a PNG file",
		},
	}
	ctx := mockContext.NewMockContext("testConfigure", "op")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.MkdirAll("data", os.ModePerm)
			assert.NoError(t, err)
			b, err := os.ReadFile(tt.image)
			assert.NoError(t, err)
			s := &imageSink{}
			err = s.Provision(ctx, tt.props)
			assert.NoError(t, err)

			err = s.saveFiles(map[string]any{
				"self": b,
			})
			if tt.err == "" {
				assert.NoError(t, err)
				entries, err := os.ReadDir("data")
				assert.NoError(t, err)
				assert.Len(t, entries, 1)
			} else {
				assert.EqualError(t, err, tt.err)
				entries, err := os.ReadDir("data")
				assert.NoError(t, err)
				assert.Len(t, entries, 0)
			}
			_ = os.RemoveAll("data")
		})
	}
}

func TestCollect(t *testing.T) {
	const Path = "test"
	s := &imageSink{}
	ctx := mockContext.NewMockContext("testSink", "op")
	err := s.Provision(ctx, map[string]any{
		"path":        Path,
		"imageFormat": "png",
		"maxCount":    1,
	})
	assert.NoError(t, err)
	b, err := os.ReadFile("../../../docs/en_US/wechat.png")
	assert.NoError(t, err)
	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	assert.NoError(t, err)
	defer s.Close(ctx)
	tests := []struct {
		n string
		d any
		e string
		c int
	}{
		{
			n: "normal",
			d: map[string]any{
				"image": b,
			},
			c: 1,
		},
		{
			n: "multiple",
			d: map[string]any{
				"image1": b,
				"image2": b,
			},
			c: 2,
		},
		{
			n: "wrong format",
			d: map[string]any{
				"wrong": "abc",
			},
			c: 0,
			e: "found none bytes data [] for path wrong",
		},
		{
			n: "list",
			d: []map[string]any{
				{
					"image1": b,
					"image2": b,
				},
				{
					"image2": b,
				},
			},
			c: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.n, func(t *testing.T) {
			switch dd := test.d.(type) {
			case map[string]any:
				err = s.Collect(ctx, &xsql.Tuple{
					Message: dd,
				})
			case []map[string]any:
				result := &xsql.WindowTuples{
					Content: make([]xsql.Row, 0, len(dd)),
				}
				for _, m := range dd {
					result.Content = append(result.Content, &xsql.Tuple{
						Message: m,
					})
				}
				err = s.CollectList(ctx, result)
			}
			if test.e == "" {
				assert.NoError(t, err)
				c, err := countFiles(Path)
				assert.NoError(t, err)
				assert.Equal(t, test.c, c)
			} else {
				assert.EqualError(t, err, test.e)
			}
			timex.Add(5 * time.Minute)
			// wait for delete files, test max count
			time.Sleep(10 * time.Millisecond)
			c, _ := countFiles(Path)
			if c > 1 {
				assert.Fail(t, "should not have more than 1 after delete files")
			}
			os.RemoveAll(Path)
			err = os.Mkdir(Path, os.ModePerm)
			assert.NoError(t, err)
		})
	}
}

func countFiles(dir string) (int, error) {
	count := 0
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	return count, err
}

func TestImagePathContainment(t *testing.T) {
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			fixture := "ekuiper.jpg"
			if format == "png" {
				fixture = "../../../docs/en_US/wechat.png"
			}
			data, err := os.ReadFile(fixture)
			require.NoError(t, err)
			for _, key := range []string{"image", "图片", "sub/image", "../escape", "sub/../../escape", filepath.Join(t.TempDir(), "absolute")} {
				t.Run(key, func(t *testing.T) {
					base := t.TempDir()
					dir := filepath.Join(base, "images")
					require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
					sink := &imageSink{c: &c{Path: dir, ImageFormat: format}}
					ctx := mockContext.NewMockContext("containment", "image")
					err := sink.Collect(ctx, &xsql.Tuple{Message: map[string]any{key: data}})
					if strings.Contains(key, "..") || filepath.IsAbs(key) {
						require.Error(t, err)
						count, err := countFiles(base)
						require.NoError(t, err)
						assert.Zero(t, count)
						return
					}
					require.NoError(t, err)
					files, err := filepath.Glob(filepath.Join(dir, key+"*."+format))
					require.NoError(t, err)
					require.Len(t, files, 1)
					encoded, err := os.ReadFile(files[0])
					require.NoError(t, err)
					if format == "png" {
						_, err = png.Decode(bytes.NewReader(encoded))
					} else {
						_, err = jpeg.Decode(bytes.NewReader(encoded))
					}
					require.NoError(t, err)
				})
			}
		})
	}
}

func TestImageSymlinkContainment(t *testing.T) {
	data, err := os.ReadFile("../../../docs/en_US/wechat.png")
	require.NoError(t, err)
	base := t.TempDir()
	dir := filepath.Join(base, "images")
	outside := filepath.Join(base, "outside")
	require.NoError(t, os.Mkdir(dir, 0o755))
	require.NoError(t, os.Mkdir(outside, 0o755))
	sentinel := filepath.Join(outside, "sentinel.png")
	require.NoError(t, os.WriteFile(sentinel, []byte("unchanged"), 0o600))
	symlink := func(target, link string) {
		t.Helper()
		err := os.Symlink(target, link)
		if err != nil && runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		require.NoError(t, err)
	}
	symlink(outside, filepath.Join(dir, "link"))
	sink := &imageSink{c: &c{Path: dir, ImageFormat: "png"}}
	require.Error(t, sink.saveFiles(map[string]any{"link/escape": data}))
	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Exercise an existing file symlink with a deterministic name at the writer.
	symlink(sentinel, filepath.Join(dir, "existing.png"))
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	defer root.Close()
	require.Error(t, sink.saveFile(root, data, "existing.png"))
	content, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	assert.Equal(t, "unchanged", string(content))

	// Links within the output tree must use relative targets, even when an
	// absolute target would point to another directory inside the same tree.
	inside := filepath.Join(dir, "inside")
	require.NoError(t, os.Mkdir(inside, 0o755))
	symlink(inside, filepath.Join(dir, "absolute-link"))
	require.Error(t, sink.saveFiles(map[string]any{"absolute-link/image": data}))
	entries, err = os.ReadDir(inside)
	require.NoError(t, err)
	require.Empty(t, entries)
	symlink("inside", filepath.Join(dir, "relative-link"))
	require.NoError(t, sink.saveFiles(map[string]any{"relative-link/image": data}))
	entries, err = os.ReadDir(inside)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	encoded, err := os.ReadFile(filepath.Join(inside, entries[0].Name()))
	require.NoError(t, err)
	_, err = png.Decode(bytes.NewReader(encoded))
	require.NoError(t, err)

	// An administrator-configured directory may itself be a symlink.
	alias := filepath.Join(base, "alias")
	symlink(dir, alias)
	sink.c.Path = alias
	require.NoError(t, sink.saveFiles(map[string]any{"normal": data}))
	files, err := filepath.Glob(filepath.Join(dir, "normal*.png"))
	require.NoError(t, err)
	assert.Len(t, files, 1)
}

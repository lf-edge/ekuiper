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

package dirwatch

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestFileDirSource(t *testing.T) {
	path, err := os.Getwd()
	require.NoError(t, err)
	fileDirSource := &FileDirSource{}
	c := map[string]interface{}{
		"path":             path,
		"allowedExtension": []string{"txt"},
	}
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	require.NoError(t, fileDirSource.Provision(ctx, c))
	require.NoError(t, fileDirSource.Connect(ctx, nil))
	output := make(chan any, 10)
	require.NoError(t, fileDirSource.Subscribe(ctx, func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		output <- data
	}, func(ctx api.StreamContext, err error) {}))
	time.Sleep(10 * time.Millisecond)
	f, err := os.Create("./test.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("123"))
	require.NoError(t, err)
	f.Close()
	got := <-output
	gotM, ok := got.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, []byte("123"), gotM["content"])
	offset, err := fileDirSource.GetOffset()
	require.NoError(t, err)
	meta := &FileDirSourceRewindMeta{}
	require.NoError(t, json.Unmarshal([]byte(offset.(string)), meta))
	require.True(t, meta.LastModifyTime.After(time.Time{}))
	require.Error(t, fileDirSource.ResetOffset(nil))
	require.NoError(t, fileDirSource.Rewind(offset))
	require.NoError(t, os.Remove("./test.txt"))
	time.Sleep(10 * time.Millisecond)
	cancel()
	fileDirSource.Close(ctx)
}

func TestCheckFileExtension(t *testing.T) {
	require.True(t, checkFileExtension("test.txt", []string{}))
	require.True(t, checkFileExtension("test.txt", []string{"txt", "jpg"}))
	require.False(t, checkFileExtension("test.md", []string{"txt", "jpg"}))
}

func TestRewind(t *testing.T) {
	fileDirSource := &FileDirSource{}
	require.Error(t, fileDirSource.Rewind(nil))
	require.Error(t, fileDirSource.Rewind("123"))
}

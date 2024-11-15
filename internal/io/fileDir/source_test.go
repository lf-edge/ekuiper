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

package fileDir

import (
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
		"path": path,
	}
	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	require.NoError(t, fileDirSource.Provision(ctx, c))
	output := make(chan []byte, 10)
	require.NoError(t, fileDirSource.Subscribe(ctx, func(ctx api.StreamContext, payload []byte, meta map[string]any, ts time.Time) {
		output <- payload
	}, func(ctx api.StreamContext, err error) {}))
	time.Sleep(10 * time.Millisecond)
	f, err := os.Create("./test.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("123"))
	require.NoError(t, err)
	f.Close()
	defer func() {
		os.Remove("./test.txt")
	}()
	data := <-output
	require.Equal(t, "123", string(data))
	os.WriteFile("./test.txt", []byte("1234"), 0666)
	data = <-output
	require.Equal(t, "1234", string(data))
	cancel()
	fileDirSource.Close(ctx)
}

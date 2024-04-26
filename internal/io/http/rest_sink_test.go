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

package http

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestRestSinkCollect(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	s := &RestSink{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"url":         fmt.Sprintf("%s/get", server.URL),
		"method":      "get",
		"compression": "gzip",
		"debugResp":   true,
	}))
	data := &xsql.Tuple{
		Message: map[string]interface{}{
			"a": 1,
		},
	}
	require.NoError(t, s.Connect(ctx))
	require.NoError(t, s.collect(ctx, data))
	require.NoError(t, s.Close(ctx))
}

func TestRestSinkRecoverErr(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	data := &xsql.Tuple{
		Message: map[string]interface{}{
			"a": 1,
		},
	}
	sErr := &RestSink{}
	require.NoError(t, sErr.Provision(ctx, map[string]any{
		"url":    fmt.Sprintf("%s/get123", server.URL),
		"method": "get",
	}))
	require.NoError(t, sErr.Connect(ctx))
	err := sErr.collect(ctx, data)
	require.Error(t, err)
	require.False(t, errorx.IsIOError(err))
	s := &RestSink{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"url":    fmt.Sprintf("%s/get", server.URL),
		"method": "get",
	}))
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/io/http/recoverAbleErr", "return(true)")
	defer failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/io/http/recoverAbleErr")
	require.NoError(t, s.Connect(ctx))
	err = s.collect(ctx, data)
	require.Error(t, err)
	require.True(t, errorx.IsIOError(err))
}

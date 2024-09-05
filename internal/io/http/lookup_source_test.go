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
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestLookup(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()
	ctx := mockContext.NewMockContext("1", "2")
	hls := &HttpLookupSource{}
	require.NoError(t, hls.Provision(ctx, map[string]any{
		"url":        server.URL,
		"datasource": "/get",
		"method":     "get",
	}))
	require.NoError(t, hls.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	got, err := hls.Lookup(ctx, []string{"code"}, []string{"code"}, []any{float64(200)})
	require.NoError(t, err)
	require.Equal(t, []map[string]any{
		{
			"code": float64(200),
		},
	}, got)
	require.NoError(t, hls.Close(ctx))
}

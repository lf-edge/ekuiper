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

package http

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func init() {
	testx.InitEnv("http")
	conf.Config.Basic.EnablePrivateNet = true
}

func TestInitConf(t *testing.T) {
	m := map[string]interface{}{}
	ctx := mockContext.NewMockContext("1", "2")
	c := &ClientConf{}
	require.NoError(t, c.InitConf(ctx, "", m))
	m = map[string]interface{}{
		"url": "",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"method": "123",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"timeout": -1,
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"timeout": -1,
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"responseType": "mock",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"method": "post",
	}
	c = &ClientConf{}
	require.NoError(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"bodyType": "123",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"url": "scae::",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"compression": "zlib",
	}
	c = &ClientConf{}
	require.NoError(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"compression": "mock",
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"oauth": map[string]any{
			"access": map[string]interface{}{
				"url":    "http://example.com/auth",
				"expire": "hello",
			},
		},
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"oauth": map[string]any{
			"access": map[string]interface{}{
				"expire": "hello",
			},
		},
	}
	c = &ClientConf{}
	require.NoError(t, c.InitConf(ctx, "", m))

	m = map[string]interface{}{
		"oauth": map[string]any{
			"refresh": map[string]interface{}{
				"expire": "hello",
			},
		},
	}
	c = &ClientConf{}
	require.Error(t, c.InitConf(ctx, "", m))
}

func TestDecode(t *testing.T) {
	testcases := []struct {
		v   interface{}
		got []map[string]interface{}
	}{
		{
			v: map[string]interface{}{
				"method": "post",
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
		{
			v: []map[string]interface{}{
				{
					"method": "post",
				},
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
		{
			v: []interface{}{
				map[string]interface{}{
					"method": "post",
				},
			},
			got: []map[string]interface{}{
				{
					"method": "post",
				},
			},
		},
	}
	for _, tc := range testcases {
		data, err := json.Marshal(tc.v)
		require.NoError(t, err)
		g, err := decode(data)
		require.NoError(t, err)
		require.Equal(t, tc.got, g)
	}
}

func TestClientAuth(t *testing.T) {
	server := createServer()
	defer func() {
		server.Close()
	}()

	c := &ClientConf{}
	ctx := mockContext.NewMockContext("1", "2")
	require.NoError(t, c.InitConf(ctx, "", map[string]interface{}{
		"oauth": map[string]interface{}{
			"access": map[string]interface{}{
				"url":    fmt.Sprintf("%s/auth", server.URL),
				"expire": "3600",
				"body":   `{"a":1}`,
			},
			"refresh": map[string]interface{}{
				"url": fmt.Sprintf("%s/refresh", server.URL),
				"headers": map[string]interface{}{
					"a": "{{.message}}",
				},
				"body": `{"a":1}`,
			},
		},
	}))
	require.NoError(t, c.auth(ctx))
	require.NoError(t, c.refresh(ctx))
}

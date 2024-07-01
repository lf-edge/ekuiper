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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestHttpPushSource(t *testing.T) {
	conf.InitConf()
	ctx := mockContext.NewMockContext("1", "2")
	s := &HttpPushSource{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"method":     "POST",
		"datasource": "/post",
	}))
	require.NoError(t, s.Connect(ctx))
	recvData := make(chan any, 10)
	require.NoError(t, s.Subscribe(ctx, func(ctx api.StreamContext, data any, meta map[string]any, ts time.Time) {
		recvData <- data
	}, func(ctx api.StreamContext, err error) {}))
	require.NoError(t, testx.TestHttp(&http.Client{}, "http://127.0.0.1:10081/post", "POST"))
	x := <-recvData
	_, ok := x.(*xsql.Tuple)
	require.True(t, ok)
	require.NoError(t, s.Close(ctx))
}

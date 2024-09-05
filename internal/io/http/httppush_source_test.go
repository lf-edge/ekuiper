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
	"encoding/hex"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/io/http/httpserver"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterConnection("httppush", httpserver.CreateConnection)
}

func TestHttpPushSource(t *testing.T) {
	connection.InitConnectionManager4Test()
	ip := "127.0.0.1"
	port := 10081
	httpserver.InitGlobalServerManager(ip, port, nil)
	defer httpserver.ShutDown()
	ctx := mockContext.NewMockContext("1", "2")
	s := &HttpPushSource{}
	require.NoError(t, s.Provision(ctx, map[string]any{
		"method":     "POST",
		"datasource": "/post",
	}))
	require.NoError(t, s.Connect(ctx, func(status string, message string) {
		// do nothing
	}))
	recvData := make(chan []byte, 10)
	require.NoError(t, s.Subscribe(ctx, func(ctx api.StreamContext, data []byte, meta map[string]any, ts time.Time) {
		recvData <- data
	}, func(ctx api.StreamContext, err error) {}))
	require.NoError(t, testx.TestHttp(&http.Client{}, fmt.Sprintf("http://%v:%v/post", ip, port), "POST"))
	x := <-recvData
	require.Equal(t, `7b0a2020202020202020227469746c65223a2022506f7374207469746c65222c0a202020202020202022626f6479223a2022506f7374206465736372697074696f6e222c0a202020202020202022757365724964223a20310a202020207d`, hex.EncodeToString(x))
	require.NoError(t, s.Close(ctx))
}

func TestHttpPushProvisionErr(t *testing.T) {
	ctx := mockContext.NewMockContext("1", "2")
	s := &HttpPushSource{}
	require.Error(t, s.Provision(ctx, map[string]any{
		"method":     "GET",
		"datasource": "/post",
	}))
	require.Error(t, s.Provision(ctx, map[string]any{
		"method":     "POST",
		"datasource": "post",
	}))
}

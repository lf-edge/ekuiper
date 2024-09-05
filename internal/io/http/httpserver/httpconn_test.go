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

package httpserver

import (
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestHttpConn(t *testing.T) {
	ip := "127.0.0.1"
	port := 10084
	InitGlobalServerManager(ip, port, nil)
	defer ShutDown()
	ctx := mockContext.NewMockContext("1", "2")
	props := map[string]any{
		"datasource": "/post",
		"method":     "POST",
	}
	c := CreateConnection(ctx)
	err := c.Provision(ctx, "test", props)
	require.NoError(t, err)
	require.NoError(t, c.Ping(ctx))
	require.NoError(t, c.Close(ctx))
}

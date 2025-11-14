// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

package redis

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestRedisPub(t *testing.T) {
	server, _ := mockRedisPubSub(false, true, DefaultChannel)
	defer server.Close()

	s := &redisPub{}
	input := [][]byte{
		[]byte(`{"humidity":50,"status":"green","temperature":22}`),
		[]byte(`{"humidity":82,"status":"wet","temperature":25}`),
		[]byte(`{"humidity":60,"status":"hot","temperature":33}`),
	}
	err := mock.RunBytesSinkCollect(s, input, map[string]any{
		"address":     addr,
		"db":          0,
		"password":    "",
		"channel":     DefaultChannel,
		"compression": "",
	})
	assert.NoError(t, err)
}

func TestSinkConfigure(t *testing.T) {
	s := RedisPub()
	prop := map[string]any{
		"address": "",
		"db":      "",
		"channel": DefaultChannel,
	}
	expErrStr := fmt.Sprintf("read properties %v fail with error: %v", prop, "1 error(s) decoding:\n\n* 'db' expected type 'int', got unconvertible type 'string', value: ''")
	ctx := mockContext.NewMockContext("testSinkConfigure", "op1")
	err := s.Provision(ctx, prop)
	if err == nil {
		t.Errorf("should have error")
		return
	} else if err.Error() != expErrStr {
		t.Errorf("error mismatch:\n\nexp=%v\n\ngot=%v\n\n", expErrStr, err.Error())
	}
}

func TestSinkPingRedisError(t *testing.T) {
	s := RedisPub().(util.PingableConn)
	prop := map[string]any{
		"address": "127.0.0.1:6379",
		"db":      0,
		"channel": DefaultChannel,
	}
	expErrStr := "Ping Redis failed with error"
	err := s.Ping(context.Background(), prop)
	if err == nil {
		t.Errorf("should have error")
		return
	} else {
		errorMsg := fmt.Sprintf("%v", err)
		parts := strings.SplitN(errorMsg, ":", 2)
		if parts[0] != expErrStr {
			t.Errorf("error mismatch:\n\nexp=%s\n\ngot=%s\n\n", expErrStr, parts[0])
		}
	}
}

func TestRedisPubDb(t *testing.T) {
	props := map[string]any{
		"db": 199,
	}
	r := &redisPub{}
	err := r.Validate(props)
	require.Error(t, err)
	require.Equal(t, "redisPub db should be in range 0-15", err.Error())
}

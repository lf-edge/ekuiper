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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestSourceConfigure(t *testing.T) {
	s := RedisSub()
	prop := map[string]any{
		"address":  "",
		"db":       "",
		"channels": []string{DefaultChannel},
	}
	expErrStr := fmt.Sprintf("read properties %v fail with error: %v", prop, "1 error(s) decoding:\n\n* 'db' expected type 'int', got unconvertible type 'string', value: ''")
	ctx := mockContext.NewMockContext("TestSourceConfigure", "op")
	err := s.Provision(ctx, prop)
	assert.EqualError(t, err, expErrStr)
}

func TestRedisDB(t *testing.T) {
	s := RedisSub()
	prop := map[string]any{
		"address":  "",
		"db":       20,
		"channels": []string{DefaultChannel},
	}
	ctx := mockContext.NewMockContext("TestRedisDB", "op")
	err := s.Provision(ctx, prop)
	assert.EqualError(t, err, "redisSub db should be in range 0-15")
}

func TestSourcePingRedisError(t *testing.T) {
	s := RedisSub().(util.PingableConn)
	prop := map[string]any{
		"address":  "",
		"db":       0,
		"channels": []string{DefaultChannel},
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

func TestRun(t *testing.T) {
	exp := []api.MessageTuple{
		model.NewDefaultRawTuple([]byte("{\"timestamp\": 1646125996000, \"node_name\": \"node1\", \"group_name\": \"group1\", \"values\": {\"tag_name1\": 11.22, \"tag_name2\": \"yellow\"}, \"errors\": {\"tag_name3\": 122}}"), map[string]any{
			"channel": "TestChannel",
		}, timex.GetNow()),
	}
	s := RedisSub()
	mock.TestSourceConnector(t, s, map[string]any{
		"address":  addr,
		"db":       0,
		"channels": []string{DefaultChannel},
	}, exp, func() {
		mockRedisPubSub(true, false, DefaultChannel)
	})
}

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

package pubsub

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"

	"github.com/lf-edge/ekuiper/internal/pkg/util"
	"github.com/lf-edge/ekuiper/pkg/api"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

func TestConnectFail(t *testing.T) {
	s := RedisSub()
	prop := map[string]interface{}{
		"address":  mr.Addr(),
		"db":       0,
		"channels": []string{DefaultChannel},
	}
	err := s.Configure("new", prop)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	ctx, cancel := mockContext.NewMockContext("ruleTestReconnect", "op1").WithCancel()
	consumer := make(chan api.SourceTuple)
	errCh := make(chan error)
	server, _ := mockRedisPubSub(false, false, DefaultChannel)
	go s.Open(ctx, consumer, errCh)
	go func() {
		select {
		case err := <-errCh:
			t.Errorf("received error: %v", err)
		}
		cancel()
	}()
	time.Sleep(1 * time.Second)
	server.Close()
	time.Sleep(1 * time.Second)
}

func TestSourceConfigure(t *testing.T) {
	s := RedisSub()
	prop := map[string]interface{}{
		"address":  "",
		"db":       "",
		"channels": []string{DefaultChannel},
	}
	expErrStr := fmt.Sprintf("read properties %v fail with error: %v", prop, "1 error(s) decoding:\n\n* 'db' expected type 'int', got unconvertible type 'string', value: ''")
	err := s.Configure("new", prop)
	if err == nil {
		t.Errorf("should have error")
		return
	} else if err.Error() != expErrStr {
		t.Errorf("error mismatch:\n\nexp=%v\n\ngot=%v\n\n", expErrStr, err.Error())
	}
}

func TestRedisDB(t *testing.T) {
	s := RedisSub()
	prop := map[string]interface{}{
		"address":  "",
		"db":       20,
		"channels": []string{DefaultChannel},
	}
	err := s.Configure("", prop)
	require.Error(t, err)
	require.Equal(t, "redisSub db should be in range 0-15", err.Error())
}

func TestSourceDecompressorError(t *testing.T) {
	s := RedisSub()
	prop := map[string]interface{}{
		"address":       "",
		"db":            0,
		"channels":      []string{DefaultChannel},
		"decompression": "test",
	}
	expErrStr := fmt.Sprintf("get decompressor test fail with error: unsupported decompressor: test")
	err := s.Configure("new", prop)
	if err == nil {
		t.Errorf("should have error")
		return
	} else if err.Error() != expErrStr {
		t.Errorf("error mismatch:\n\nexp=%v\n\ngot=%v\n\n", expErrStr, err.Error())
	}
}

func TestSourcePingRedisError(t *testing.T) {
	s := RedisSub().(util.PingableConn)
	prop := map[string]interface{}{
		"address":  "",
		"db":       0,
		"channels": []string{DefaultChannel},
	}
	expErrStr := fmt.Sprintf("Ping Redis failed with error")
	err := s.Ping("new", prop)
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

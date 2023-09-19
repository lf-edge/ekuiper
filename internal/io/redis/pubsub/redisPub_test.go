// Copyright 2023-2023 emy120115@gmail.com
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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/lf-edge/ekuiper/internal/io/mock"
	mockContext "github.com/lf-edge/ekuiper/internal/io/mock/context"
)

func TestRedisPub(t *testing.T) {
	server, ch := mockRedisPubSub(false, true, DefaultChannel)
	defer server.Close()

	s := RedisPub()
	s.Configure(map[string]interface{}{
		"address":     addr,
		"db":          0,
		"password":    "",
		"channel":     DefaultChannel,
		"compression": "",
	})

	data := []interface{}{
		map[string]interface{}{
			"temperature": 22,
			"humidity":    50,
			"status":      "green",
		},
		map[string]interface{}{
			"temperature": 25,
			"humidity":    82,
			"status":      "wet",
		},
		map[string]interface{}{
			"temperature": 33,
			"humidity":    60,
			"status":      "hot",
		},
	}
	err := mock.RunSinkCollect(s, data)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	exp := []string{
		`{"humidity":50,"status":"green","temperature":22}`,
		`{"humidity":82,"status":"wet","temperature":25}`,
		`{"humidity":60,"status":"hot","temperature":33}`,
	}

	var actual []string
	ticker := time.After(10 * time.Second)
	for i := 0; i < len(exp); i++ {
		select {
		case <-ticker:
			t.Errorf("timeout")
			return
		case d := <-ch:
			actual = append(actual, string(d))
		}
	}

	if !reflect.DeepEqual(actual, exp) {
		t.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", exp, actual)
	}
	time.Sleep(100 * time.Millisecond)
}

func TestSinkConnExp(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Error(err)
	}
	mport := m.Port()
	s := RedisPub()
	s.Configure(map[string]interface{}{
		"address":     m.Addr(),
		"db":          0,
		"password":    "",
		"channel":     DefaultChannel,
		"compression": "",
	})
	data := []interface{}{
		map[string]interface{}{
			"temperature": 22,
			"humidity":    50,
			"status":      "green",
		},
	}
	ctx := mockContext.NewMockContext("ruleSink", "op1")
	err = s.Open(ctx)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second)
	m.Close()
	expErrStr := fmt.Sprintf("io error: Error occurred while publishing the Redis message to 127.0.0.1:%s", mport)
	for _, e := range data {
		err := s.Collect(ctx, e)
		if err == nil {
			t.Errorf("should have error")
			return
		} else if err.Error() != expErrStr {
			t.Errorf("error mismatch:\n\nexp=%s\n\ngot=%s\n\n", expErrStr, err.Error())
		}
	}
	fmt.Println("closing sink")
	err = s.Close(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestSinkConfigure(t *testing.T) {
	s := RedisPub()
	prop := map[string]interface{}{
		"address": "",
		"db":      "",
		"channel": DefaultChannel,
	}
	expErrStr := fmt.Sprintf("read properties %v fail with error: %v", prop, "1 error(s) decoding:\n\n* 'db' expected type 'int', got unconvertible type 'string', value: ''")
	err := s.Configure(prop)
	if err == nil {
		t.Errorf("should have error")
		return
	} else if err.Error() != expErrStr {
		t.Errorf("error mismatch:\n\nexp=%v\n\ngot=%v\n\n", expErrStr, err.Error())
	}
}

func TestSinkDecompressorError(t *testing.T) {
	s := RedisPub()
	prop := map[string]interface{}{
		"address":     "",
		"db":          0,
		"channel":     DefaultChannel,
		"compression": "test",
	}
	expErrStr := fmt.Sprintf("invalid compression method test")
	err := s.Configure(prop)
	if err == nil {
		t.Errorf("should have error")
		return
	} else if err.Error() != expErrStr {
		t.Errorf("error mismatch:\n\nexp=%v\n\ngot=%v\n\n", expErrStr, err.Error())
	}
}

func TestSinkPingRedisError(t *testing.T) {
	s := RedisPub()
	prop := map[string]interface{}{
		"address": "127.0.0.1:6379",
		"db":      0,
		"channel": DefaultChannel,
	}
	expErrStr := fmt.Sprintf("Ping Redis failed with error")
	err := s.Configure(prop)
	if err != nil {
		t.Error(err)
	}
	ctx := mockContext.NewMockContext("ruleSink", "op1")
	err = s.Open(ctx)
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

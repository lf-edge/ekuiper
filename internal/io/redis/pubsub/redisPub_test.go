// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"testing"
	"time"

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
	}
	ctx := mockContext.NewMockContext("ruleSink", "op1")
	err := s.Open(ctx)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second)
	mr.Close()
	expErrStr := fmt.Sprintf("io error: Error occurred while publishing the Redis message to localhost:%s: dial tcp 127.0.0.1:%s: connectex: No connection could be made because the target machine actively refused it.", port, port)
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

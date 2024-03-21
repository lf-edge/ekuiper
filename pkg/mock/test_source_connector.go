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

package mock

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/pkg/api"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

func TestSourceConnector(t *testing.T, r api.SourceConnector, expected []api.SourceTuple, sender func()) {
	// init
	c := count.Load()
	if c == nil {
		count.Store(1)
		c = 0
	}
	ctx, cancel := mockContext.NewMockContext(fmt.Sprintf("rule%d", c), "op1").WithCancel()
	count.Store(c.(int) + 1)
	consumer := make(chan api.SourceTuple)
	ctrlCh := make(chan error)
	// connect, subscribe and read data
	err := r.Connect(ctx)
	assert.NoError(t, err)
	go r.Open(ctx, consumer, ctrlCh)
	defer func() {
		err = r.Close(ctx)
		assert.NoError(t, err)
	}()
	err = r.Subscribe(ctx)
	assert.NoError(t, err)
	// Send data
	go func() {
		sender()
	}()
	// Receive data
	limit := len(expected)
	ticker := time.After(2 * time.Second)
	var result []api.SourceTuple
outerloop:
	for {
		select {
		case sg := <-ctrlCh:
			switch et := sg.(type) {
			case error:
				cancel()
				assert.Fail(t, et.Error())
			default:
				fmt.Println("ctrlCh", et)
			}
		case tuple := <-consumer:
			result = append(result, tuple)
			limit--
			if limit <= 0 {
				break outerloop
			}
		case <-ticker:
			cancel()
			assert.Fail(t, "timeout")
		}
	}
	cancel()
	assert.Equal(t, expected, result)
}

// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

var count atomic.Value

func TestSourceOpen(r api.Source, exp []api.SourceTuple, t *testing.T) {
	result, err := RunMockSource(r, len(exp))
	if err != nil {
		t.Error(err)
	}
	for i, v := range result {
		switch v.(type) {
		case *api.DefaultSourceTuple:
			assert.Equal(t, exp[i].Message(), v.Message())
			assert.Equal(t, exp[i].Meta(), v.Meta())
		case *xsql.ErrorSourceTuple:
			assert.Equal(t, reflect.TypeOf(exp[i]), reflect.TypeOf(v))
			assert.Equal(t, exp[i].(*xsql.ErrorSourceTuple).Error, v.(*xsql.ErrorSourceTuple).Error)
		default:
			assert.Equal(t, exp[i], v)
		}
	}
}

func RunMockSource(r api.Source, limit int) ([]api.SourceTuple, error) {
	c := count.Load()
	if c == nil {
		count.Store(1)
		c = 0
	}
	ctx, cancel := mockContext.NewMockContext(fmt.Sprintf("rule%d", c), "op1").WithCancel()
	cv, _ := converter.GetOrCreateConverter(&ast.Options{FORMAT: "json"})
	ctx = context.WithValue(ctx.(*context.DefaultContext), context.DecodeKey, cv)
	count.Store(c.(int) + 1)
	consumer := make(chan api.SourceTuple)
	errCh := make(chan error)
	go r.Open(ctx, consumer, errCh)
	ticker := time.After(10 * time.Second)
	var result []api.SourceTuple
outerloop:
	for {
		select {
		case err := <-errCh:
			cancel()
			return nil, err
		case tuple := <-consumer:
			result = append(result, tuple)
			limit--
			if limit <= 0 {
				break outerloop
			}
		case <-ticker:
			cancel()
			return nil, fmt.Errorf("timeout")
		}
	}
	err := r.Close(ctx)
	if err != nil {
		return nil, err
	}
	cancel()
	return result, nil
}

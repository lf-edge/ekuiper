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
	"errors"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func RunBytesSinkCollect(s api.BytesCollector, data [][]byte, props map[string]any) error {
	ctx := mockContext.NewMockContext("ruleSink", "op1")
	err := s.Provision(ctx, props)
	if err != nil {
		return err
	}
	err = s.Connect(ctx)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	for _, e := range data {
		err = s.Collect(ctx, &xsql.Tuple{Rawdata: e})
		if err != nil {
			return err
		}
	}
	time.Sleep(time.Second)
	fmt.Println("closing sink")
	return s.Close(ctx)
}

func RunTupleSinkCollect(s api.TupleCollector, data []any, props map[string]any) error {
	ctx := mockContext.NewMockContext("ruleSink", "op1")
	err := s.Provision(ctx, props)
	if err != nil {
		return err
	}
	err = s.Connect(ctx)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	for _, e := range data {
		switch ee := e.(type) {
		case api.SinkTuple:
			err = s.Collect(ctx, ee)
		case api.SinkTupleList:
			err = s.CollectList(ctx, ee)
		// TODO Make the output all as tuple
		case map[string]any:
			err = s.Collect(ctx, model.NewDefaultSourceTuple(ee, nil, timex.GetNow()))
		case []map[string]any:
			tuples := make([]api.SinkTuple, 0, len(ee))
			for _, m := range ee {
				tuples = append(tuples, model.NewDefaultSourceTuple(m, nil, timex.GetNow()))
			}
			err = s.CollectList(ctx, MemTupleList(tuples))
		default:
			err = errors.New("unsupported data type")
		}
		if err != nil {
			return err
		}
	}
	time.Sleep(time.Second)
	fmt.Println("closing sink")
	return s.Close(ctx)
}

type MemTupleList []api.SinkTuple

func (l MemTupleList) RangeOfTuples(f func(index int, tuple api.SinkTuple) bool) {
	for i, v := range l {
		if !f(i, v) {
			break
		}
	}
}

func (l MemTupleList) Len() int {
	return len(l)
}

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
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func RunBytesSinkCollect(s api.BytesCollector, data [][]byte) error {
	ctx := mockContext.NewMockContext("ruleSink", "op1")
	err := s.Connect(ctx)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	for _, e := range data {
		err = s.Collect(ctx, e)
		if err != nil {
			return err
		}
	}
	time.Sleep(time.Second)
	fmt.Println("closing sink")
	return s.Close(ctx)
}

func RunTupleSinkCollect(s api.TupleCollector, data []any) error {
	ctx := mockContext.NewMockContext("ruleSink", "op1")
	err := s.Connect(ctx)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	for _, e := range data {
		switch ee := e.(type) {
		case api.Tuple:
			err = s.Collect(ctx, ee)
		case []api.Tuple:
			err = s.CollectList(ctx, ee)
		// TODO Make the output all as tuple
		case api.ReadonlyMessage:
			err = s.Collect(ctx, api.NewDefaultSourceTuple(ee, nil, timex.GetNow()))
		case []api.ReadonlyMessage:
			tuples := make([]api.Tuple, 0, len(ee))
			for _, m := range ee {
				tuples = append(tuples, api.NewDefaultSourceTuple(m, nil, timex.GetNow()))
			}
			err = s.CollectList(ctx, tuples)
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

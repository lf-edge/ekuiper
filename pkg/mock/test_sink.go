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
)

func RunBytesSinkCollect(s api.BytesCollector, data [][]byte, props map[string]any) error {
	ctx := mockContext.NewMockContext("ruleSink", "op1")
	err := s.Provision(ctx, props)
	if err != nil {
		return err
	}
	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	for _, e := range data {
		err = s.Collect(ctx, &xsql.RawTuple{Rawdata: e})
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
	err = s.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	for _, e := range data {
		switch ee := e.(type) {
		case api.MessageTupleList:
			err = s.CollectList(ctx, ee)
		case api.MessageTuple:
			err = s.Collect(ctx, ee)
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

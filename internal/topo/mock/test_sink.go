// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/pkg/api"
	"time"
)

func RunSinkCollect(s api.Sink, data []interface{}) error {
	ctx := NewMockContext("ruleSink", "op1")
	err := s.Open(ctx)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	for _, e := range data {
		err := s.Collect(ctx, e)
		if err != nil {
			return err
		}
	}
	time.Sleep(time.Second)
	fmt.Println("closing sink")
	return s.Close(ctx)
}

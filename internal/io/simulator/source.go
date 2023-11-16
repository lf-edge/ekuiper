// Copyright 2023 EMQ Technologies Co., Ltd.
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

package simulator

import (
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type c struct {
	Data     []map[string]any `json:"data"`
	Interval int              `json:"interval"`
	Loop     bool             `json:"loop"`
}

type Source struct {
	c *c
}

func (m *Source) Configure(_ string, props map[string]interface{}) error {
	err := cast.MapToStruct(props, &m.c)
	if err != nil {
		return err
	}
	if len(m.c.Data) == 0 {
		return fmt.Errorf("data cannot be empty")
	}
	if m.c.Interval < 1 {
		return fmt.Errorf("interval must be greater than 1 ms, got %d", m.c.Interval)
	}
	return nil
}

func (m *Source) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	ctx.GetLogger().Infof("trail run source starts")
	ticker := time.NewTicker(time.Duration(m.c.Interval) * time.Millisecond)
	defer ticker.Stop()
	index := 0
	index = m.send(ctx, consumer, index)
	for {
		select {
		case <-ticker.C:
			if index >= len(m.c.Data) {
				if !m.c.Loop {
					// rule stop signal
					infra.DrainError(ctx, nil, errCh)
					return
				}
				index = 0
			}
			index = m.send(ctx, consumer, index)
		case <-ctx.Done():
			return
		}
	}
}

func (m *Source) send(ctx api.StreamContext, consumer chan<- api.SourceTuple, index int) int {
	tuple := api.NewDefaultSourceTupleWithTime(m.c.Data[index], nil, conf.GetNow())
	select {
	case consumer <- tuple:
		index++
	case <-ctx.Done():
	}
	return index
}

func (m *Source) Close(_ api.StreamContext) error {
	return nil
}

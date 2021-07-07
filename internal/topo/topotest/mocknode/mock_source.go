// Copyright 2021 EMQ Technologies Co., Ltd.
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

package mocknode

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"sync"
	"time"
)

type MockSource struct {
	data   []*xsql.Tuple
	offset int
	sync.Mutex
}

const TIMELEAP = 200

func (m *MockSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	log := ctx.GetLogger()
	mockClock := mockclock.GetMockClock()
	log.Infof("%d: mock source %s starts", conf.GetNowInMilli(), ctx.GetOpId())
	log.Debugf("mock source %s starts with offset %d", ctx.GetOpId(), m.offset)
	for i, d := range m.data {
		if i < m.offset {
			log.Debugf("mock source is skipping %d", i)
			continue
		}
		log.Debugf("mock source is waiting %d", i)
		diff := d.Timestamp - conf.GetNowInMilli()
		if diff <= 0 {
			log.Warnf("Time stamp invalid, current time is %d, but timestamp is %d", conf.GetNowInMilli(), d.Timestamp)
			diff = TIMELEAP
		}
		next := mockClock.After(time.Duration(diff) * time.Millisecond)
		//Mock timer, only send out the data once the mock time goes to the timestamp.
		//Another mechanism must be imposed to move forward the mock time.
		select {
		case <-next:
			m.Lock()
			m.offset = i + 1
			consumer <- api.NewDefaultSourceTuple(d.Message, xsql.Metadata{"topic": "mock"})
			log.Debugf("%d: mock source %s is sending data %d:%s", cast.TimeToUnixMilli(mockClock.Now()), ctx.GetOpId(), i, d)
			m.Unlock()
		case <-ctx.Done():
			log.Debugf("mock source open DONE")
			return
		}
	}
	log.Debugf("mock source sends out all data")
}

func (m *MockSource) GetOffset() (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	return m.offset, nil
}

func (m *MockSource) Rewind(offset interface{}) error {
	oi, err := cast.ToInt(offset, cast.STRICT)
	if err != nil {
		return fmt.Errorf("mock source fails to rewind: %s", err)
	} else {
		m.offset = oi
	}
	return nil
}

func (m *MockSource) Close(_ api.StreamContext) error {
	m.offset = 0
	return nil
}

func (m *MockSource) Configure(dataKey string, _ map[string]interface{}) error {
	m.data = TestData[dataKey]
	return nil
}

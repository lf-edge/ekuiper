package test

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"time"
)

type MockSource struct {
	data        []*xsql.Tuple
	done        <-chan int
	isEventTime bool

	offset int
}

func NewMockSource(data []*xsql.Tuple, done <-chan int, isEventTime bool) *MockSource {
	mock := &MockSource{
		data:        data,
		done:        done,
		isEventTime: isEventTime,
	}
	return mock
}

func (m *MockSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	log := ctx.GetLogger()
	mockClock := GetMockClock()
	log.Debugf("mock source starts with offset %d", m.offset)
	for i, d := range m.data {
		if i < m.offset {
			log.Debugf("mock source is skipping %d", i)
			continue
		}
		log.Debugf("mock source is waiting", i)
		select {
		case j, ok := <-m.done:
			if ok {
				log.Debugf("mock source receives data %d", j)
			} else {
				log.Debugf("sync channel done at %d", i)
			}
		case <-ctx.Done():
			log.Debugf("mock source open DONE")
			return
		}

		if !m.isEventTime {
			mockClock.Set(common.TimeFromUnixMilli(d.Timestamp))
			log.Debugf("set time at %d", d.Timestamp)
		} else {
			mockClock.Add(1000 * time.Millisecond)
		}

		select {
		case <-ctx.Done():
			log.Debugf("mock source open DONE")
			return
		default:
		}

		consumer <- api.NewDefaultSourceTuple(d.Message, xsql.Metadata{"topic": "mock"})
		log.Debugf("mock source is sending data %s", d)
		m.offset = i + 1
		time.Sleep(1)
	}
	log.Debugf("mock source sends out all data")
}

func (m *MockSource) GetOffset() (interface{}, error) {
	return m.offset, nil
}

func (m *MockSource) Rewind(offset interface{}) error {
	oi, err := common.ToInt(offset)
	if err != nil {
		return fmt.Errorf("mock source fails to rewind: %s", err)
	} else {
		m.offset = oi
	}
	return nil
}

func (m *MockSource) Close(ctx api.StreamContext) error {
	m.offset = 0
	return nil
}

func (m *MockSource) Configure(topic string, props map[string]interface{}) error {
	return nil
}

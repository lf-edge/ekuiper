package test

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"time"
)

type MockSource struct {
	data        []*xsql.Tuple
	done        chan<- struct{}
	isEventTime bool
}

// New creates a new CsvSource
func NewMockSource(data []*xsql.Tuple, done chan<- struct{}, isEventTime bool) *MockSource {
	mock := &MockSource{
		data:        data,
		done:        done,
		isEventTime: isEventTime,
	}
	return mock
}

func (m *MockSource) Open(ctx api.StreamContext, consume api.ConsumeFunc) (err error) {
	log := ctx.GetLogger()
	mockClock := GetMockClock()
	log.Debugln("mock source starts")
	go func() {
		for _, d := range m.data {
			log.Debugf("mock source is sending data %s", d)
			if !m.isEventTime {
				mockClock.Set(common.TimeFromUnixMilli(d.Timestamp))
			}else {
				mockClock.Add(1000 * time.Millisecond)
			}
			consume(d.Message, nil)
			time.Sleep(1)
		}
		if m.isEventTime{
			mockClock.Add(1000 * time.Millisecond)
			time.Sleep(1)
		}
		m.done <- struct{}{}
	}()
	return nil
}

func (m *MockSource) Close(ctx api.StreamContext) error {
	return nil
}

func (m *MockSource) Configure(topic string, props map[string]interface{}) error {
	return nil
}

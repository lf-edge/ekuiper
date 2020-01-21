package test

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"time"
)

type MockSource struct {
	data        []*xsql.Tuple
	done        <-chan int
	isEventTime bool
}

// New creates a new CsvSource
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
	log.Debugln("mock source starts")
	for _, d := range m.data {
		<-m.done
		log.Debugf("mock source is sending data %s", d)
		if !m.isEventTime {
			mockClock.Set(common.TimeFromUnixMilli(d.Timestamp))
		} else {
			mockClock.Add(1000 * time.Millisecond)
		}
		consumer <- api.NewDefaultSourceTuple(d.Message, nil)
		time.Sleep(1)
	}
}

func (m *MockSource) Close(ctx api.StreamContext) error {
	return nil
}

func (m *MockSource) Configure(topic string, props map[string]interface{}) error {
	return nil
}

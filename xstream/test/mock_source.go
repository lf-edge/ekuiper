package test

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"time"
)

type MockSource struct {
	data []*xsql.Tuple
	done chan<- struct{}
	isEventTime bool
}

// New creates a new CsvSource
func NewMockSource(data []*xsql.Tuple, done chan<- struct{}, isEventTime bool) *MockSource {
	mock := &MockSource{
		data: data,
		done: done,
		isEventTime: isEventTime,
	}
	return mock
}

func (m *MockSource) Open(ctx api.StreamContext, consume api.ConsumeFunc) (err error) {
	log := ctx.GetLogger()

	log.Trace("mock source starts")
	go func(){
		for _, d := range m.data{
			log.Infof("mock source is sending data %s", d)
			if !m.isEventTime{
				common.SetMockNow(d.Timestamp)
				ticker := common.GetMockTicker()
				timer := common.GetMockTimer()
				if ticker != nil {
					ticker.DoTick(d.Timestamp)
				}
				if timer != nil {
					timer.DoTick(d.Timestamp)
				}
			}
			consume(d.Message, nil)
			if m.isEventTime{
				time.Sleep(1000 * time.Millisecond) //Let window run to make sure timers are set
			}else{
				time.Sleep(50 * time.Millisecond) //Let window run to make sure timers are set
			}

		}
		if !m.isEventTime {
			//reset now for the next test
			common.SetMockNow(0)
		}
		m.done <- struct{}{}
	}()
	return nil
}

func (m *MockSource) Close(ctx api.StreamContext) error{
	return nil
}

func (m *MockSource) Configure(topic string, props map[string]interface{}) error {
	return nil
}
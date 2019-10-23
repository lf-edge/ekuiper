package test

import (
	"context"
	"engine/common"
	"engine/xsql"
	"time"
)

type MockSource struct {
	outs map[string]chan<- interface{}
	data []*xsql.Tuple
	name string
	done chan<- struct{}
	isEventTime bool
}

// New creates a new CsvSource
func NewMockSource(data []*xsql.Tuple, name string, done chan<- struct{}, isEventTime bool) *MockSource {
	mock := &MockSource{
		data: data,
		name: name,
		outs: make(map[string]chan<- interface{}),
		done: done,
		isEventTime: isEventTime,
	}
	return mock
}

func (m *MockSource) Open(ctx context.Context) (err error) {
	log := common.GetLogger(ctx)
	log.Trace("Mocksource starts")
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
			for _, out := range m.outs{
				select {
				case out <- d:
				case <-ctx.Done():
					log.Trace("Mocksource stop")
					return
//				default:  TODO non blocking must have buffer?
				}
				time.Sleep(50 * time.Millisecond)
			}
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

func (m *MockSource) AddOutput(output chan<- interface{}, name string) {
	if _, ok := m.outs[name]; !ok{
		m.outs[name] = output
	}else{
		common.Log.Warnf("fail to add output %s, operator %s already has an output of the same name", name, m.name)
	}
}

package test

import (
	"context"
	"engine/common"
)

type MockSink struct {
	ruleId   string
	name 	 string
	results  [][]byte
	input chan interface{}
}

func NewMockSink(name, ruleId string) *MockSink{
	m := &MockSink{
		ruleId:  ruleId,
		name:    name,
		input: make(chan interface{}),
	}
	return m
}

func (m *MockSink) Open(ctx context.Context, result chan<- error) {
	log := common.GetLogger(ctx)
	log.Trace("Opening mock sink")
	m.results = make([][]byte, 0)
	go func() {
		for {
			select {
			case item := <-m.input:
				if v, ok := item.([]byte); ok {
					log.Infof("mock sink receive %s", item)
					m.results = append(m.results, v)
				}else{
					log.Info("mock sink receive non byte data")
				}

			case <-ctx.Done():
				log.Infof("mock sink %s done", m.name)
				return
			}
		}
	}()
}

func (m *MockSink) GetInput() (chan<- interface{}, string)  {
	return m.input, m.name
}

func (m *MockSink) GetResults() [][]byte {
	return m.results
}

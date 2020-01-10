package test

import (
	"github.com/emqx/kuiper/xstream/api"
)

type MockSink struct {
	results [][]byte
}

func NewMockSink() *MockSink {
	m := &MockSink{}
	return m
}

func (m *MockSink) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Debugln("Opening mock sink")
	m.results = make([][]byte, 0)
	return nil
}

func (m *MockSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if v, ok := item.([]byte); ok {
		logger.Debugf("mock sink receive %s", item)
		m.results = append(m.results, v)
	} else {
		logger.Info("mock sink receive non byte data")
	}
	return nil
}

func (m *MockSink) Close(ctx api.StreamContext) error {
	//do nothing
	return nil
}

func (m *MockSink) Configure(props map[string]interface{}) error {
	return nil
}

func (m *MockSink) GetResults() [][]byte {
	return m.results
}

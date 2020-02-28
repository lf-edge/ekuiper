package main

import "github.com/emqx/kuiper/xstream/api"

type memory struct {
	results [][]byte
}

func (m *memory) Open(ctx api.StreamContext) error {
	log := ctx.GetLogger()
	log.Debug("Opening memory sink")
	m.results = make([][]byte, 0)
	return nil
}

func (m *memory) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if v, ok := item.([]byte); ok {
		logger.Debugf("memory sink receive %s", item)
		m.results = append(m.results, v)
	} else {
		logger.Debug("memory sink receive non byte data")
	}
	return nil
}

func (m *memory) Close(ctx api.StreamContext) error {
	//do nothing
	return nil
}

func (m *memory) Configure(props map[string]interface{}) error {
	return nil
}

var Memory memory

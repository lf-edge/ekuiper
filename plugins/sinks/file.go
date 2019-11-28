package main

import (
	"bufio"
	"context"
	"engine/xstream/api"
	"fmt"
	"os"
	"sync"
	"time"
)

type fileSink struct {
	interval int
	path string

	results  [][]byte
	file *os.File
	mux sync.Mutex
	cancel context.CancelFunc
}

func (m *fileSink) Configure(props map[string]interface{}) error {
	m.interval = 1000
	m.path = "cache"
	if i, ok := props["interval"]; ok {
		if i, ok := i.(int); ok {
			m.interval = i
		}
	}
	if i, ok := props["path"]; ok {
		if i, ok := i.(string); ok {
			m.path = i
		}
	}
	return nil
}

func (m *fileSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Debug("Opening file sink")
	m.results = make([][]byte, 0)
	var f *os.File
	var err error
	if _, err := os.Stat(m.path); os.IsNotExist(err) {
		f, err = os.Create(m.path)
	}else{
		f, err = os.Open(m.path)
	}
	if err != nil{
		return fmt.Errorf("fail to open file sink for %v", err)
	}
	m.file = f
	t := time.NewTicker(time.Duration(m.interval) * time.Millisecond)
	exeCtx, cancel := ctx.WithCancel()
	m.cancel = cancel
	go func(){
		defer t.Stop()
		for{
			select{
			case <- t.C:
				m.save(logger)
			case <-exeCtx.Done():
				logger.Info("file sink done")
				return
			}
		}
	}()
	return nil
}

func (m *fileSink) save(logger api.Logger) {
	var strings []string
	m.mux.Lock()
	for _, b := range m.results {
		strings = append(strings, string(b)+"\n")
	}
	m.results = make([][]byte, 0)
	m.mux.Unlock()
	w := bufio.NewWriter(m.file)
	for _, s := range strings {
		_, err := m.file.WriteString(s)
		if err != nil {
			logger.Errorf("file sink fails to write out result: %s", s)
		}
	}
	w.Flush()
}

func (m *fileSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if v, ok := item.([]byte); ok {
		logger.Debugf("memory sink receive %s", item)
		m.mux.Lock()
		m.results = append(m.results, v)
		m.mux.Unlock()
	}else{
		logger.Debug("memory sink receive non byte data")
	}
	return nil
}

func (m *fileSink) Close(ctx api.StreamContext) error {
	if m.cancel != nil {
		m.cancel()
	}
	if m.file != nil{
		m.save(ctx.GetLogger())
		return m.file.Close()
	}
	return nil
}

var File fileSink
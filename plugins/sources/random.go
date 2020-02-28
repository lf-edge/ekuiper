package main

import (
	"context"
	"github.com/emqx/kuiper/xstream/api"
	"math/rand"
	"time"
)

//Emit data randomly with only a string field
type randomSource struct {
	interval int
	seed     int
	pattern  map[string]interface{}
	cancel   context.CancelFunc
}

func (s *randomSource) Configure(topic string, props map[string]interface{}) error {
	if i, ok := props["interval"].(float64); ok {
		s.interval = int(i)
	} else {
		s.interval = 1000
	}
	if p, ok := props["pattern"].(map[string]interface{}); ok {
		s.pattern = p
	} else {
		s.pattern = make(map[string]interface{})
		s.pattern["count"] = 50
	}
	if i, ok := props["seed"].(float64); ok {
		s.seed = int(i)
	} else {
		s.seed = 1
	}
	return nil
}

func (s *randomSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	t := time.NewTicker(time.Duration(s.interval) * time.Millisecond)
	exeCtx, cancel := ctx.WithCancel()
	s.cancel = cancel
	defer t.Stop()
	for {
		select {
		case <-t.C:
			consumer <- api.NewDefaultSourceTuple(randomize(s.pattern, s.seed), nil)
		case <-exeCtx.Done():
			return
		}
	}
}

func randomize(p map[string]interface{}, seed int) map[string]interface{} {
	r := make(map[string]interface{})
	for k, v := range p {
		vi := v.(int)
		r[k] = vi + rand.Intn(seed)
	}
	return r
}

func (s *randomSource) Close(ctx api.StreamContext) error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

var Random randomSource

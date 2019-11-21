package main

import (
	"context"
	"engine/xstream/api"
	"time"
)

//Emit data randomly with only a string field
type randomSource struct {
	interval int
	pattern map[string]interface{}
	cancel context.CancelFunc
}

func (s *randomSource) Configure(topic string, props map[string]interface{}) error{
	if i, ok := props["interval"].(int); ok{
		s.interval = i
	}else{
		s.interval = 1000
	}
	if p, ok := props["pattern"].(map[string]interface{}); ok{
		s.pattern = p
	}else{
		s.pattern = make(map[string]interface{})
		s.pattern["count"] = 50
	}
	return nil
}

func (s *randomSource) Open(ctx api.StreamContext, consume api.ConsumeFunc) (err error) {
	t := time.NewTicker(time.Duration(s.interval) * time.Millisecond)
	exeCtx, cancel := ctx.WithCancel()
	s.cancel = cancel
	go func(exeCtx api.StreamContext){
		defer t.Stop()
		for{
			select{
			case <- t.C:
				consume(randomize(s.pattern), nil)
			case <- exeCtx.Done():
				return
			}
		}
	}(exeCtx)
	return nil
}

func randomize(p map[string]interface{}) map[string]interface{}{
	r := make(map[string]interface{})
	for k, v := range p{
		vi := v.(int)
		r[k] = vi + 1
	}
	return r
}

func (s *randomSource) Close(ctx api.StreamContext) error{
	if s.cancel != nil{
		s.cancel()
	}
	return nil
}

var Random randomSource
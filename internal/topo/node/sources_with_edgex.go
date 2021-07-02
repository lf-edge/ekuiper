// +build edgex
// +build !test

package node

import (
	"github.com/emqx/kuiper/internal/topo/sink"
	"github.com/emqx/kuiper/internal/topo/source"
	"github.com/emqx/kuiper/pkg/api"
)

func getSource(t string) (api.Source, error) {
	if t == "edgex" {
		return &source.EdgexSource{}, nil
	}
	return doGetSource(t)
}

func getSink(name string, action map[string]interface{}) (api.Sink, error) {
	if name == "edgex" {
		s := &sink.EdgexMsgBusSink{}
		if err := s.Configure(action); err != nil {
			return nil, err
		} else {
			return s, nil
		}
	}
	return doGetSink(name, action)
}

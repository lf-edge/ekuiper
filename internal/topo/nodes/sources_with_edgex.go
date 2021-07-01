// +build edgex
// +build !test

package nodes

import (
	"github.com/emqx/kuiper/internal/topo/extensions"
	"github.com/emqx/kuiper/internal/topo/sinks"
	"github.com/emqx/kuiper/pkg/api"
)

func getSource(t string) (api.Source, error) {
	if t == "edgex" {
		return &extensions.EdgexSource{}, nil
	}
	return doGetSource(t)
}

func getSink(name string, action map[string]interface{}) (api.Sink, error) {
	if name == "edgex" {
		s := &sinks.EdgexMsgBusSink{}
		if err := s.Configure(action); err != nil {
			return nil, err
		} else {
			return s, nil
		}
	}
	return doGetSink(name, action)
}

// +build test
// +build edgex

package nodes

import (
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/extensions"
	"github.com/emqx/kuiper/xstream/sinks"
	"github.com/emqx/kuiper/xstream/topotest/mocknodes"
)

func getSource(t string) (api.Source, error) {
	if t == "edgex" {
		return &extensions.EdgexSource{}, nil
	} else if t == "mock" {
		return &mocknodes.MockSource{}, nil
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

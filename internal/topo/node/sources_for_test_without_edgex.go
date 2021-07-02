// +build !edgex
// +build test

package node

import (
	"github.com/lf-edge/ekuiper/internal/topo/topotest/mocknode"
	"github.com/lf-edge/ekuiper/pkg/api"
)

func getSource(t string) (api.Source, error) {
	if t == "mock" {
		return &mocknode.MockSource{}, nil
	}
	return doGetSource(t)
}

func getSink(name string, action map[string]interface{}) (api.Sink, error) {
	return doGetSink(name, action)
}

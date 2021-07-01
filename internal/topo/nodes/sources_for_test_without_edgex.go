// +build !edgex
// +build test

package nodes

import (
	"github.com/emqx/kuiper/internal/topo/topotest/mocknodes"
	"github.com/emqx/kuiper/pkg/api"
)

func getSource(t string) (api.Source, error) {
	if t == "mock" {
		return &mocknodes.MockSource{}, nil
	}
	return doGetSource(t)
}

func getSink(name string, action map[string]interface{}) (api.Sink, error) {
	return doGetSink(name, action)
}

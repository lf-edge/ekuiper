// +build !edgex

package nodes

import "github.com/emqx/kuiper/xstream/api"

func getSource(t string) (api.Source, error) {
	return doGetSource(t)
}

func getSink(name string, action map[string]interface{}) (api.Sink, error) {
	return doGetSink(name, action)
}
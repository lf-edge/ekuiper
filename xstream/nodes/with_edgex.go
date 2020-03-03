// +build edgex

package nodes

import (
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/extensions"
)

func getSource(t string) (api.Source, error) {
	if t == "edgex" {
		return &extensions.EdgexSource{}, nil
	}
	return doGetSource(t)
}

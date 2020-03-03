// +build !linux

package nodes

import "github.com/emqx/kuiper/xstream/api"

func getSource(t string) (api.Source, error) {
	return doGetSource(t)
}

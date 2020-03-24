package plugin_manager

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"path"
	"plugin"
	"unicode"
)

var registry map[string]plugin.Symbol

func init() {
	registry = make(map[string]plugin.Symbol)
}

func GetPlugin(t string, ptype string) (plugin.Symbol, error) {
	t = ucFirst(t)
	key := ptype + "/" + t
	var nf plugin.Symbol
	nf, ok := registry[key]
	if !ok {
		loc, err := common.GetLoc("/plugins/")
		if err != nil {
			return nil, fmt.Errorf("cannot find the plugins folder")
		}
		mod := path.Join(loc, ptype, t+".so")
		plug, err := plugin.Open(mod)
		if err != nil {
			return nil, fmt.Errorf("cannot open %s: %v", mod, err)
		}
		nf, err = plug.Lookup(t)
		if err != nil {
			return nil, fmt.Errorf("cannot find symbol %s, please check if it is exported", t)
		}
		registry[key] = nf
	}
	return nf, nil
}

func ucFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}

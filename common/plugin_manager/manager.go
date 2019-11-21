package plugin_manager

import (
	"fmt"
	"plugin"
	"unicode"
)

var registry map[string]plugin.Symbol

func init(){
	registry = make(map[string]plugin.Symbol)
}

func GetPlugin(t string, ptype string) (plugin.Symbol, error) {
	t = ucFirst(t)
	key := ptype + "/" + t
	var nf plugin.Symbol
	nf, ok := registry[key]
	if !ok {
		mod := "plugins/" + key + ".so"
		plug, err := plugin.Open(mod)
		if err != nil {
			return nil, fmt.Errorf("cannot open %s: %v", mod, err)
		}
		nf, err = plug.Lookup(t)
		if err != nil {
			return nil, fmt.Errorf("cannot find symbol %s, please check if it is exported", t)
		}
	}
	return nf, nil
}

func ucFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}


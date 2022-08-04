package wasm

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
)

type PluginInfo struct {
	runtime.PluginMeta
	Functions []string `json:"functions"`
}

var langMap = map[string]bool{
	"go":     true,
	"python": true,
}

func (p *PluginInfo) Validate(expectedName string) error {
	if p.Name != expectedName {
		return fmt.Errorf("invalid plugin, expect name '%s' but got '%s'", expectedName, p.Name)
	}
	if p.Language == "" {
		return fmt.Errorf("invalid plugin, missing language")
	}
	if p.Executable == "" {
		return fmt.Errorf("invalid plugin, missing executable")
	}
	if len(p.Functions) == 0 {
		return fmt.Errorf("invalid plugin, must define at lease one function")
	}
	if l, ok := langMap[p.Language]; !ok || !l {
		return fmt.Errorf("invalid plugin, language '%s' is not supported", p.Language)
	}
	return nil
}

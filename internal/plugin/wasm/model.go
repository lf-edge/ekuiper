package wasm

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
)

type PluginInfo struct {
	runtime.PluginMeta
	Functions []string `json:"functions"`
}

func (p *PluginInfo) Validate(expectedName string) error {
	if p.Name != expectedName {
		return fmt.Errorf("invalid plugin, expect name '%s' but got '%s'", expectedName, p.Name)
	}
	if len(p.Functions) == 0 {
		return fmt.Errorf("invalid plugin, must define at lease one function")
	}
	if p.WasmEngine == "" {
		return fmt.Errorf("invalid WasmEngine")
	}
	return nil
}

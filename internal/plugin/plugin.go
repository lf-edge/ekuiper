// Copyright 2021 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plugin

import "encoding/json"

type PluginType int

const (
	SOURCE PluginType = iota
	SINK
	FUNCTION
	PORTABLE
	WASM
)

var PluginTypes = []string{"sources", "sinks", "functions", "portable", "wasm"}

var PluginTypeMap = map[string]PluginType{
	"sources":   SOURCE,
	"sinks":     SINK,
	"functions": FUNCTION,
	"portable":  PORTABLE,
	"wasm":      WASM,
}

type Plugin interface {
	GetName() string
	GetFile() string
	GetShellParas() []string
	GetSymbols() []string
	SetName(n string)
	GetInstallScripts() []byte
}

// IOPlugin Unify model. Flat all properties for each kind.
type IOPlugin struct {
	Name       string   `json:"name"`
	File       string   `json:"file"`
	ShellParas []string `json:"shellParas"`
}

func (p *IOPlugin) GetName() string {
	return p.Name
}

func (p *IOPlugin) GetFile() string {
	return p.File
}

func (p *IOPlugin) GetShellParas() []string {
	return p.ShellParas
}

func (p *IOPlugin) GetSymbols() []string {
	return nil
}

func (p *IOPlugin) SetName(n string) {
	p.Name = n
}

func (p *IOPlugin) GetInstallScripts() []byte {
	marshal, err := json.Marshal(p)
	if err != nil {
		return nil
	}
	return marshal
}

func NewPluginByType(t PluginType) Plugin {
	switch t {
	case FUNCTION:
		return &FuncPlugin{}
	case WASM:
		return &FuncPlugin{}
	default:
		return &IOPlugin{}
	}
}

type FuncPlugin struct {
	IOPlugin
	// Optional, if not specified, a default element with the same name of the file will be registered
	Functions []string `json:"functions"`
}

func (fp *FuncPlugin) GetSymbols() []string {
	return fp.Functions
}

type EXTENSION_TYPE int

const (
	NONE_EXTENSION EXTENSION_TYPE = iota
	INTERNAL
	NATIVE_EXTENSION
	PORTABLE_EXTENSION
	SERVICE_EXTENSION
	WASM_EXTENSION
)

package runtime

type Meta struct {
	RuleId     string `json:"ruleId"`
	OpId       string `json:"opId"`
	InstanceId int    `json:"instanceId"`
}

type FuncMeta struct {
	Meta
	FuncId int `json:"funcId"`
}

type Control struct {
	SymbolName string                 `json:"symbolName"`
	Meta       *Meta                  `json:"meta,omitempty"`
	PluginType string                 `json:"pluginType"`
	DataSource string                 `json:"dataSource,omitempty"`
	Config     map[string]interface{} `json:"config,omitempty"`
}

type Command struct {
	Cmd string `json:"cmd"`
	Arg string `json:"arg"`
}

type FuncData struct {
	Func string      `json:"func"`
	Arg  interface{} `json:"arg"`
}

type FuncReply struct {
	State  bool        `json:"state"`
	Result interface{} `json:"result"`
}

type PluginMeta struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	WasmFile   string `json:"wasmFile"`
	WasmEngine string `json:"wasmEngine"`
}

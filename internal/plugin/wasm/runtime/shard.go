package runtime

const (
	TYPE_SOURCE = "source"
	TYPE_SINK   = "sink"
	TYPE_FUNC   = "func"
)

type Meta struct {
	RuleId     string `json:"ruleId"`
	OpId       string `json:"opId"`
	InstanceId int    `json:"instanceId"`
}

type FuncMeta struct {
	Meta
	FuncId int `json:"funcId"`
	//FuncName string `json:"symbolName"`
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

const (
	CMD_START = "start"
	CMD_STOP  = "stop"
)

const (
	REPLY_OK = "ok"
)

type WasmConfig struct {
	SendTimeout int64 `json:"sendTimeout"`
}

type FuncData struct {
	Func string      `json:"func"`
	Arg  interface{} `json:"arg"`
}

type FuncReply struct {
	State  bool        `json:"state"`
	Result interface{} `json:"result"`
}

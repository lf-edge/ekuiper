package internal

const (
	KuiperFieldPREFIX string = "kuiper_field_"

	//PRIVATE_PREFIX string = "$$"

	Separator = "__"
)
const (
	StreamLogFile = "stream.log"
)

const (
	KuiperConf     = "kuiper.yaml"
	ClientConf     = "client.yaml"
	ConnectionConf = "connections/connection.yaml"
	JsonFileSuffix = ".json"
	YamlFileSuffix = ".yaml"
	ZipFileSuffix  = ".zip"
	SoFileSuffix   = ".so"
)

//internal/conf
const (
	EtcDir     = "etc"
	DataDir    = "data"
	LogDir     = "log"
	PluginsDir = "plugins"

	EnvKuiperBaseKey   = "KuiperBaseKey"
	EnvKuiperSyslogKey = "KuiperSyslogKey"
)

// internal/meta
const (
	Sink                             = "sink"
	Source                           = "source"
	Func                             = "func"
	Start                            = "start"
	Stop                             = "stop"
	SourceCfgOperatorKeyTemplate     = "sources.%s"
	ConnectionCfgOperatorKeyTemplate = "connections.%s"
)

// internal/plugin
const (
	Delete = "$deleted"
)

// internal/server
const (
	EtcOsRelease    = "/etc/os-release"
	UsrLibOsRelease = "/usr/lib/os-release"
	ContentType     = "Content-Type"
	ContentTypeJSON = "application/json"
	QueryRuleId     = "internal-ekuiper_query_rule"
)

// internal/topo
const (
	DecodeKey   = "$$decode"
	LoggerKey   = "$$logger"
	TransKey    = "$$trans"
	OffsetKey   = "$$offset"
	BatchKey    = "$$batchInputs"
	NeuronTopic = "$$neuron"
	NeuronUrl   = "ipc:///tmp/neuron-ekuiper.ipc"
	Topic       = "topic"
)

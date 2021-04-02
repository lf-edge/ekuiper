package services

type (
	protocol string
	schema   string
)

const (
	REST    protocol = "rest"
	GRPC             = "grpc"
	MSGPACK          = "msgpack-rpc"
)

const (
	PROTOBUFF schema = "protobuf"
)

type (
	author struct {
		Name    string `json:"name"`
		Email   string `json:"email"`
		Company string `json:"company"`
		Website string `json:"website"`
	}
	fileLanguage struct {
		English string `json:"en_US"`
		Chinese string `json:"zh_CN"`
	}
	about struct {
		Author      *author       `json:"author"`
		HelpUrl     *fileLanguage `json:"helpUrl"`
		Description *fileLanguage `json:"description"`
	}
	mapping struct {
		Name        string        `json:"name"`
		ServiceName string        `json:"serviceName"`
		Description *fileLanguage `json:"description"`
	}
	binding struct {
		Name        string        `json:"name"`
		Description *fileLanguage `json:"description"`
		Address     string        `json:"address"`
		Protocol    protocol      `json:"protocol"`
		SchemaType  schema        `json:"schemaType"`
		SchemaFile  string        `json:"schemaFile"`
		Functions   []*mapping    `json:"functions"`
	}

	conf struct {
		About      *about     `json:"about"`
		Interfaces []*binding `json:"interfaces"`
	}
)

// The external function's location, currently service.interface.
type serviceInfo struct {
	About      *about
	Interfaces map[string]*interfaceInfo
}

type schemaInfo struct {
	SchemaType schema
	SchemaFile string
}

type interfaceInfo struct {
	Desc      *fileLanguage
	Addr      string
	Protocol  protocol
	Schema    *schemaInfo
	Functions []string
}

type functionContainer struct {
	ServiceName   string
	InterfaceName string
	MethodName    string
}

type FunctionExec struct {
	Protocol protocol
	Addr     string
}

package modules

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type SchemaTypeDef interface {
	Scan(logger api.Logger, path string) (map[string]*Files, error)
	Infer(logger api.Logger, filePath string, messageId string) (ast.StreamFields, error)
}

// SchemaTypeDefs is the registry of all schema type.
var SchemaTypeDefs = map[string]SchemaTypeDef{}

func RegisterSchemaType(name string, t SchemaTypeDef) {
	SchemaTypeDefs[name] = t
}

const (
	PROTOBUF = "protobuf"
	CUSTOM   = "custom"
)

type Files struct {
	SchemaFile string
	SoFile     string
}

//go:build schema || !core

package io

import (
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterSchemaType(modules.PROTOBUF, &schema.PbType{})
	modules.RegisterSchemaType(modules.CUSTOM, &schema.CustomType{})
}

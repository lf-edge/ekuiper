// Copyright 2022 EMQ Technologies Co., Ltd.
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

//go:build !core || (rpc && schema)
// +build !core rpc,schema

package server

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/def"
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/lf-edge/ekuiper/internal/schema"
)

func (t *Server) CreateSchema(arg *model.RPCTypedArgDesc, reply *string) error {
	sd := &schema.Info{Type: def.SchemaType(arg.Type)}
	if arg.Json != "" {
		if err := json.Unmarshal([]byte(arg.Json), sd); err != nil {
			return fmt.Errorf("Parse service %s error : %s.", arg.Json, err)
		}
	}
	if sd.Name != arg.Name {
		return fmt.Errorf("Create schema error: name mismatch.")
	}
	if sd.Content != "" && sd.FilePath != "" {
		return fmt.Errorf("Invalid body: Cannot specify both content and file")
	}
	err := schema.Register(sd)
	if err != nil {
		return fmt.Errorf("Create schema error: %s", err)
	} else {
		*reply = fmt.Sprintf("Schema %s is created.", arg.Name)
	}
	return nil
}

func (t *Server) DescSchema(arg *model.RPCTypedArgDesc, reply *string) error {
	j, err := schema.GetSchema(def.SchemaType(arg.Type), arg.Name)
	if err != nil {
		return fmt.Errorf("Desc schema error : %s.", err)
	} else if j == nil {
		return fmt.Errorf("Desc schema error : not found.")
	} else {
		r, err := marshalDesc(j)
		if err != nil {
			return fmt.Errorf("Describe service error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) DropSchema(arg *model.RPCTypedArgDesc, reply *string) error {
	err := schema.DeleteSchema(def.SchemaType(arg.Type), arg.Name)
	if err != nil {
		return fmt.Errorf("Drop schema error : %s.", err)
	}
	*reply = fmt.Sprintf("Schema %s is dropped", arg.Name)
	return nil
}

func (t *Server) ShowSchemas(schemaType string, reply *string) error {
	l, err := schema.GetAllForType(def.SchemaType(schemaType))
	if err != nil {
		return fmt.Errorf("Show schemas error: %s.", err)
	}
	if len(l) == 0 {
		*reply = "No schema definitions are found."
	} else {
		r, err := marshalDesc(l)
		if err != nil {
			return fmt.Errorf("Show service error: %v", err)
		}
		*reply = r
	}
	return nil
}

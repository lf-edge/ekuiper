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

//go:build !core || (rpc && service)
// +build !core rpc,service

package server

import (
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/internal/hack"
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/lf-edge/ekuiper/internal/service"
)

func (t *Server) CreateService(arg *model.RPCArgDesc, reply *string) error {
	sd := &service.ServiceCreationRequest{}
	if arg.Json != "" {
		if err := json.Unmarshal(hack.StringToBytes(arg.Json), sd); err != nil {
			return fmt.Errorf("Parse service %s error : %s.", arg.Json, err)
		}
	}
	if sd.Name != arg.Name {
		return fmt.Errorf("Create service error: name mismatch.")
	}
	if sd.File == "" {
		return fmt.Errorf("Create service error: Missing service file url.")
	}
	err := serviceManager.Create(sd)
	if err != nil {
		return fmt.Errorf("Create service error: %s", err)
	} else {
		*reply = fmt.Sprintf("Service %s is created.", arg.Name)
	}
	return nil
}

func (t *Server) DescService(name string, reply *string) error {
	s, err := serviceManager.Get(name)
	if err != nil {
		return fmt.Errorf("Desc service error : %s.", err)
	} else {
		r, err := marshalDesc(s)
		if err != nil {
			return fmt.Errorf("Describe service error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) DescServiceFunc(name string, reply *string) error {
	s, err := serviceManager.GetFunction(name)
	if err != nil {
		return fmt.Errorf("Desc service func error : %s.", err)
	} else {
		r, err := marshalDesc(s)
		if err != nil {
			return fmt.Errorf("Describe service func error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) DropService(name string, reply *string) error {
	err := serviceManager.Delete(name)
	if err != nil {
		return fmt.Errorf("Drop service error : %s.", err)
	}
	*reply = fmt.Sprintf("Service %s is dropped", name)
	return nil
}

func (t *Server) ShowServices(_ int, reply *string) error {
	s, err := serviceManager.List()
	if err != nil {
		return fmt.Errorf("Show service error: %s.", err)
	}
	if len(s) == 0 {
		*reply = "No service definitions are found."
	} else {
		r, err := marshalDesc(s)
		if err != nil {
			return fmt.Errorf("Show service error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) ShowServiceFuncs(_ int, reply *string) error {
	s, err := serviceManager.ListFunctions()
	if err != nil {
		return fmt.Errorf("Show service funcs error: %s.", err)
	}
	if len(s) == 0 {
		*reply = "No service definitions are found."
	} else {
		r, err := marshalDesc(s)
		if err != nil {
			return fmt.Errorf("Show service funcs error: %v", err)
		}
		*reply = r
	}
	return nil
}

// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

//go:build full || (rpc && script)

package server

import (
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/plugin/js"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/validate"
)

func (t *Server) CreateScript(j string, reply *string) error {
	sd := &js.Script{}
	if err := json.Unmarshal(cast.StringToBytes(j), sd); err != nil {
		return fmt.Errorf("Parse JavaScript function error : %s.", err)
	}
	if err := validate.ValidateID(sd.Id); err != nil {
		return err
	}
	err := js.GetManager().Create(sd)
	if err != nil {
		return fmt.Errorf("Create JavaScript function error: %s", err)
	} else {
		*reply = fmt.Sprintf("JavaScript function %s is created.", sd.Id)
	}
	return nil
}

func (t *Server) DescScript(name string, reply *string) error {
	if err := validate.ValidateID(name); err != nil {
		return err
	}
	j, err := js.GetManager().GetScript(name)
	if err != nil {
		return fmt.Errorf("Describe JavaScript function error : %s.", err)
	} else {
		r, err := marshalDesc(j)
		if err != nil {
			return fmt.Errorf("Describe JavaScript function error : %s.", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) DropScript(name string, reply *string) error {
	if err := validate.ValidateID(name); err != nil {
		return err
	}
	err := js.GetManager().Delete(name)
	if err != nil {
		return fmt.Errorf("Drop JavaScript function error : %s.", err)
	}
	*reply = fmt.Sprintf("JavaScript function %s is dropped", name)
	return nil
}

func (t *Server) ShowScripts(_ int, reply *string) error {
	content, err := js.GetManager().List()
	if err != nil {
		return fmt.Errorf("Show JavaScript functions error: %s.", err)
	}
	if len(content) == 0 {
		*reply = "No JavaScript functions are found."
	} else {
		r, err := marshalDesc(content)
		if err != nil {
			return fmt.Errorf("Show service error: %v", err)
		}
		*reply = r
	}
	return nil
}

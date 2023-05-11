// Copyright 2023 EMQ Technologies Co., Ltd.
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

//go:build script

package planner

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/topo/graph"
	"github.com/lf-edge/ekuiper/internal/topo/operator"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func init() {
	extNodes["script"] = func(name string, props map[string]interface{}, options *api.RuleOption) (api.TopNode, error) {
		sop, err := parseScript(props)
		if err != nil {
			return nil, err
		}
		op := Transform(sop, name, options)
		return op, nil
	}
}

func parseScript(props map[string]interface{}) (*operator.ScriptOp, error) {
	n := &graph.Script{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	if n.Script == "" {
		return nil, fmt.Errorf("script node must have script")
	}
	return operator.NewScriptOp(n.Script, n.IsAgg)
}

// Copyright 2026 EMQ Technologies Co., Ltd.
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

package server

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

type ShowScanResponse struct {
	Emitter          string         `json:"emitter"`
	ScanTableContent map[string]any `json:"content"`
}

func rulesShowScanTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ruleName := vars["name"]

	topo, err := registry.GetRulePlainTopo(ruleName)
	if err != nil {
		handleError(w, err, fmt.Sprintf("failed to get topology for rule %s", ruleName), logger)
		return
	}

	ops := topo.GetOperators()
	joinNode := extractJoinNode(ops)
	if joinNode == nil {
		handleError(w, fmt.Errorf("join node is not found for %s", ruleName), "scan table error", logger)
		return
	}

	tuples := joinNode.CaptureSnapshot()
	if tuples == nil || tuples.Len() == 0 {
		handleError(w, fmt.Errorf("unable to find tuples for the given scan table"), "scan table error", logger)
		return
	}

	res := make([]ShowScanResponse, tuples.Len())
	for index, row := range tuples.Content {
		if t, ok := row.(*xsql.Tuple); ok {
			res[index].Emitter = t.Emitter
			res[index].ScanTableContent = t.Message
		}
	}

	jsonResponse(res, w, logger)
}

func extractJoinNode(ops []node.OperatorNode) *node.JoinAlignNode {
	for _, opNode := range ops {
		if joinAlignNode, ok := opNode.(*node.JoinAlignNode); ok {
			return joinAlignNode
		}
	}

	return nil
}

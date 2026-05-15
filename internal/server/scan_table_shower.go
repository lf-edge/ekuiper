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
	defer r.Body.Close()
	vars := mux.Vars(r)
	ruleName := vars["name"]

	topo, err := registry.GetRulePlainTopo(ruleName)
	if err != nil {
		handleError(w, err, "", logger)
		return
	}

	ops := topo.GetOperators()
	joinNode := extractJoinNode(ops)
	if joinNode == nil {
		return
	}

	tuples := joinNode.CaptureSnapshot()
	if tuples.Len() == 0 {
		handleError(w, fmt.Errorf(""), "", logger)
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

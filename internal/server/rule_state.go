// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/topo/rule/machine"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type UpdateRuleStateType int

const (
	UpdateRuleState UpdateRuleStateType = iota
	UpdateRuleOffset
)

func ruleStateHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	ruleID := vars["name"]
	req := &ruleStateUpdateRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		handleError(w, err, "", logger)
		return
	}
	var err error
	switch req.StateType {
	case int(UpdateRuleOffset):
		err = updateRuleOffset(ruleID, req.Params)
	default:
		err = fmt.Errorf("unknown stateType:%v", req.StateType)
	}
	failpoint.Inject("updateOffset", func(val failpoint.Value) {
		switch val.(int) {
		case 3:
			err = nil
		}
	})
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("success"))
}

type ruleStateUpdateRequest struct {
	StateType int                    `json:"type"`
	Params    map[string]interface{} `json:"params"`
}

type resetOffsetRequest struct {
	StreamName string                 `json:"streamName"`
	Input      map[string]interface{} `json:"input"`
}

func updateRuleOffset(ruleID string, param map[string]interface{}) error {
	s, StateErr := getRuleState(ruleID)
	failpoint.Inject("updateOffset", func(val failpoint.Value) {
		switch val.(int) {
		case 1:
			StateErr = nil
			s = machine.Running
		case 2:
			StateErr = nil
			s = machine.Stopped
		}
	})
	if StateErr != nil {
		return StateErr
	}
	if s != machine.Running {
		return fmt.Errorf("rule %v should be running when modify state", ruleID)
	}

	req := &resetOffsetRequest{}
	if err := cast.MapToStruct(param, req); err != nil {
		return err
	}
	rs, ok := registry.load(ruleID)
	if !ok {
		return fmt.Errorf("rule %s is not found in registry", ruleID)
	}
	return rs.ResetStreamOffset(req.StreamName, req.Input)
}

// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"sort"
	"sync"
)

var registry *RuleRegistry

type RuleState struct {
	Name      string
	Topology  *topo.Topo
	Triggered bool
	// temporary storage for topo graph to make sure even rule close, the graph is still available
	topoGraph *topo.PrintableTopo
}

func (rs *RuleState) GetTopoGraph() *topo.PrintableTopo {
	if rs.topoGraph != nil {
		return rs.topoGraph
	} else if rs.Topology != nil {
		return rs.Topology.GetTopo()
	} else {
		return nil
	}
}

// Stop Assume rule has started and the topo has instantiated
func (rs *RuleState) Stop() {
	rs.Triggered = false
	rs.Topology.Cancel()
	rs.topoGraph = rs.Topology.GetTopo()
	rs.Topology = nil
}

type RuleRegistry struct {
	sync.RWMutex
	internal map[string]*RuleState
}

func (rr *RuleRegistry) Store(key string, value *RuleState) {
	rr.Lock()
	rr.internal[key] = value
	rr.Unlock()
}

func (rr *RuleRegistry) Load(key string) (value *RuleState, ok bool) {
	rr.RLock()
	result, ok := rr.internal[key]
	rr.RUnlock()
	return result, ok
}

// Delete Atomic get and delete
func (rr *RuleRegistry) Delete(key string) (*RuleState, bool) {
	rr.Lock()
	result, ok := rr.internal[key]
	if ok {
		delete(rr.internal, key)
	}
	rr.Unlock()
	return result, ok
}

func createRuleState(rule *api.Rule) (*RuleState, error) {
	rs := &RuleState{
		Name: rule.Id,
	}
	registry.Store(rule.Id, rs)
	if tp, err := planner.Plan(rule); err != nil {
		return rs, err
	} else {
		rs.Topology = tp
		rs.Triggered = true
		return rs, nil
	}
}

// Assume rs is started with topo instantiated
func doStartRule(rs *RuleState) error {
	err := ruleProcessor.ExecReplaceRuleState(rs.Name, true)
	if err != nil {
		return err
	}
	go func() {
		tp := rs.Topology
		err := infra.SafeRun(func() error {
			select {
			case err := <-tp.Open():
				return err
			}
		})
		if err != nil {
			tp.GetContext().SetError(err)
			logger.Errorf("closing rule %s for error: %v", rs.Name, err)
			tp.Cancel()
			rs.Triggered = false
		} else {
			rs.Triggered = false
			logger.Infof("closing rule %s", rs.Name)
		}
	}()
	return nil
}

func getAllRulesWithStatus() ([]map[string]interface{}, error) {
	names, err := ruleProcessor.GetAllRules()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	result := make([]map[string]interface{}, len(names))
	for i, name := range names {
		s, err := getRuleState(name)
		if err != nil {
			s = fmt.Sprintf("error: %s", err)
		}
		result[i] = map[string]interface{}{
			"id":     name,
			"status": s,
		}
	}
	return result, nil
}

func getRuleState(name string) (string, error) {
	if rs, ok := registry.Load(name); ok {
		return doGetRuleState(rs)
	} else {
		return "", fmt.Errorf("Rule %s is not found in registry", name)
	}
}

func doGetRuleState(rs *RuleState) (string, error) {
	result := ""
	if rs.Topology == nil {
		result = "Stopped: canceled manually or by error."
	} else {
		c := (*rs.Topology).GetContext()
		if c != nil {
			err := c.Err()
			switch err {
			case nil:
				result = "Running"
			case context.Canceled:
				result = "Stopped: canceled by error."
			case context.DeadlineExceeded:
				result = "Stopped: deadline exceed."
			default:
				result = fmt.Sprintf("Stopped: %v.", err)
			}
		} else {
			result = "Stopped: no context found."
		}
	}
	return result, nil
}

func getRuleStatus(name string) (string, error) {
	if rs, ok := registry.Load(name); ok {
		result, err := doGetRuleState(rs)
		if err != nil {
			return "", err
		}
		if result == "Running" {
			keys, values := (*rs.Topology).GetMetrics()
			metrics := "{"
			for i, key := range keys {
				value := values[i]
				switch value.(type) {
				case string:
					metrics += fmt.Sprintf("\"%s\":%q,", key, value)
				default:
					metrics += fmt.Sprintf("\"%s\":%v,", key, value)
				}
			}
			metrics = metrics[:len(metrics)-1] + "}"
			dst := &bytes.Buffer{}
			if err = json.Indent(dst, []byte(metrics), "", "  "); err != nil {
				result = metrics
			} else {
				result = dst.String()
			}
		}
		return result, nil
	} else {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func getRuleTopo(name string) (string, error) {
	if rs, ok := registry.Load(name); ok {
		graph := rs.GetTopoGraph()
		if graph == nil {
			return "", errorx.New(fmt.Sprintf("Fail to get rule %s's topo, make sure the rule has been started before", name))
		}
		bs, err := json.Marshal(graph)
		if err != nil {
			return "", errorx.New(fmt.Sprintf("Fail to encode rule %s's topo", name))
		} else {
			return string(bs), nil
		}
	} else {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func startRule(name string) error {
	var rs *RuleState
	rs, ok := registry.Load(name)
	if !ok || (!rs.Triggered) {
		r, err := ruleProcessor.GetRuleByName(name)
		if err != nil {
			return err
		}
		rs, err = createRuleState(r)
		if err != nil {
			return err
		}
		err = doStartRule(rs)
		if err != nil {
			return err
		}
	} else {
		conf.Log.Warnf("Rule %s is already started", name)
	}
	return nil
}

func stopRule(name string) (result string) {
	if rs, ok := registry.Load(name); ok && rs.Triggered {
		rs.Stop()
		err := ruleProcessor.ExecReplaceRuleState(name, false)
		if err != nil {
			conf.Log.Warnf("stop rule found error: %s", err.Error())
		}
		result = fmt.Sprintf("Rule %s was stopped.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func deleteRule(name string) (result string) {
	if rs, ok := registry.Delete(name); ok {
		if rs.Triggered {
			(*rs.Topology).Cancel()
		}
		result = fmt.Sprintf("Rule %s was deleted.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func restartRule(name string) error {
	stopRule(name)
	return startRule(name)
}

func recoverRule(name string) string {
	rule, err := ruleProcessor.GetRuleByName(name)
	if err != nil {
		return fmt.Sprintf("%v", err)
	}

	if !rule.Triggered {
		rs := &RuleState{
			Name: name,
		}
		registry.Store(name, rs)
		return fmt.Sprintf("Rule %s was stoped.", name)
	}

	err = startRule(name)
	if err != nil {
		return fmt.Sprintf("%v", err)
	}
	return fmt.Sprintf("Rule %s was started.", name)

}

// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"sort"
	"sync"
	"time"
)

// Rule storage includes kv and in memory registry
// Kv stores the rule text with *expected* status so that the rule can be restored after restart
// Registry stores the current rule state in runtime
// Here registry is the in memory registry
var registry *RuleRegistry

type RuleRegistry struct {
	sync.RWMutex
	internal map[string]*rule.RuleState
}

// Store create the in memory entry for a rule. Run in:
// 1. Restore the rules from KV at startup
// 2. Restore the rules when importing
// 3. Create a rule
func (rr *RuleRegistry) Store(key string, value *rule.RuleState) {
	rr.Lock()
	rr.internal[key] = value
	rr.Unlock()
}

// Load the entry of a rule by id. It is used to get the current rule state
// or send command to a running rule
func (rr *RuleRegistry) Load(key string) (value *rule.RuleState, ok bool) {
	rr.RLock()
	result, ok := rr.internal[key]
	rr.RUnlock()
	return result, ok
}

// Delete Atomic get and delete. Only run when deleting a rule in runtime.
func (rr *RuleRegistry) Delete(key string) (*rule.RuleState, bool) {
	rr.Lock()
	result, ok := rr.internal[key]
	if ok {
		delete(rr.internal, key)
	}
	rr.Unlock()
	return result, ok
}

func createRule(name, ruleJson string) (string, error) {
	// Validate the rule json
	r, err := ruleProcessor.GetRuleByJson(name, ruleJson)
	if err != nil {
		return "", fmt.Errorf("invalid rule json: %v", err)
	}
	// Store to KV
	err = ruleProcessor.ExecCreate(r.Id, ruleJson)
	if err != nil {
		return r.Id, fmt.Errorf("store the rule error: %v", err)
	}

	// Validate the topo
	rs, err := createRuleState(r)
	if err != nil {
		// Do not store to registry so also delete the KV
		deleteRule(r.Id)
		_, _ = ruleProcessor.ExecDrop(r.Id)
		return r.Id, fmt.Errorf("create rule topo error: %v", err)
	}

	// Start the rule asyncly
	if r.Triggered {
		go func() {
			panicOrError := infra.SafeRun(func() error {
				//Start the rule which runs async
				return rs.Start()
			})
			if panicOrError != nil {
				logger.Errorf("Rule %s start failed: %s", r.Id, panicOrError)
			}
		}()
	}
	return r.Id, nil
}

// Create and initialize a rule state.
// Errors are possible during plan the topo.
// If error happens return immediately without add it to the registry
func createRuleState(r *api.Rule) (*rule.RuleState, error) {
	rs, err := rule.NewRuleState(r)
	if err != nil {
		return rs, err
	}
	registry.Store(r.Id, rs)
	return rs, nil
}

func recoverRule(r *api.Rule) string {
	// Validate the topo
	rs, err := createRuleState(r)
	if err != nil { // when recovering rules, assume the rules are valid, so always add it to the registry
		conf.Log.Errorf("Create rule topo error: %v", err)
		r.Triggered = false
		registry.Store(r.Id, rs)
	}
	if !r.Triggered {
		return fmt.Sprintf("Rule %s was stopped.", r.Id)
	} else {
		panicOrError := infra.SafeRun(func() error {
			//Start the rule which runs async
			return rs.Start()
		})
		if panicOrError != nil {
			return fmt.Sprintf("Rule %s start failed: %s", r.Id, panicOrError)
		}
	}
	return fmt.Sprintf("Rule %s was started.", r.Id)
}

func updateRule(ruleId, ruleJson string) error {
	// Validate the rule json
	r, err := ruleProcessor.GetRuleByJson(ruleId, ruleJson)
	if err != nil {
		return fmt.Errorf("Invalid rule json: %v", err)
	}
	if rs, ok := registry.Load(r.Id); ok {
		err := rs.UpdateTopo(r)
		if err != nil {
			return err
		}
		err = ruleProcessor.ExecReplaceRuleState(rs.RuleId, true)
		return err
	} else {
		return fmt.Errorf("Rule %s registry not found, try to delete it and recreate", r.Id)
	}
}

func deleteRule(name string) (result string) {
	if rs, ok := registry.Delete(name); ok {
		rs.Close()
		result = fmt.Sprintf("Rule %s was deleted.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func startRule(name string) error {
	rs, ok := registry.Load(name)
	if !ok {
		return fmt.Errorf("Rule %s is not found in registry, please check if it is created", name)
	} else {
		err := rs.Start()
		if err != nil {
			return err
		}
		err = ruleProcessor.ExecReplaceRuleState(rs.RuleId, true)
		return err
	}
}

func stopRule(name string) (result string) {
	if rs, ok := registry.Load(name); ok {
		err := rs.Stop()
		if err != nil {
			conf.Log.Warn(err)
		}
		err = ruleProcessor.ExecReplaceRuleState(name, false)
		if err != nil {
			conf.Log.Warnf("stop rule found error: %s", err.Error())
		}
		result = fmt.Sprintf("Rule %s was stopped.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func restartRule(name string) error {
	stopRule(name)
	time.Sleep(1 * time.Millisecond)
	return startRule(name)
}

func getRuleStatus(name string) (string, error) {
	if rs, ok := registry.Load(name); ok {
		result, err := rs.GetState()
		if err != nil {
			return "", err
		}
		if result == "Running" {
			keys, values := (*rs.Topology).GetMetrics()
			metrics := "{"
			metrics += `"status": "running",`
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
		} else {
			result = fmt.Sprintf(`{"status": "stopped", "message": "%s"}`, result)
		}
		return result, nil
	} else {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func getAllRulesWithStatus() ([]map[string]interface{}, error) {
	ruleIds, err := ruleProcessor.GetAllRules()
	if err != nil {
		return nil, err
	}
	sort.Strings(ruleIds)
	result := make([]map[string]interface{}, len(ruleIds))
	for i, id := range ruleIds {
		ruleName := id
		rule, _ := ruleProcessor.GetRuleById(id)
		if rule != nil && rule.Name != "" {
			ruleName = rule.Name
		}
		s, err := getRuleState(id)
		if err != nil {
			s = fmt.Sprintf("error: %s", err)
		}
		result[i] = map[string]interface{}{
			"id":     id,
			"name":   ruleName,
			"status": s,
		}
	}
	return result, nil
}

func getRuleState(name string) (string, error) {
	if rs, ok := registry.Load(name); ok {
		return rs.GetState()
	} else {
		return "", fmt.Errorf("Rule %s is not found in registry", name)
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

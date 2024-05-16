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
	"sort"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/meta"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/server/promMetrics"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/internal/topo/rule"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/hidden"
	"github.com/lf-edge/ekuiper/pkg/infra"
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

func createRule(name, ruleJson string) (id string, err error) {
	var rs *rule.RuleState = nil

	// Validate the rule json
	r, err := ruleProcessor.GetRuleByJson(name, ruleJson)
	if err != nil {
		return "", fmt.Errorf("invalid rule json: %v", err)
	}

	if exists := ruleProcessor.ExecExists(r.Id); exists {
		return r.Id, fmt.Errorf("rule %v already exists", r.Id)
	}

	// Validate the topo
	err = infra.SafeRun(func() error {
		rs, err = createRuleState(r)
		return err
	})
	if err != nil {
		return r.Id, err
	}
	defer func() {
		if err != nil {
			// Do not store to registry so also delete the KV
			deleteRule(id)
		}
	}()

	// Store to KV
	err = ruleProcessor.ExecCreate(r.Id, ruleJson)
	if err != nil {
		return r.Id, fmt.Errorf("store the rule error: %v", err)
	}

	// Start the rule asyncly
	if r.Triggered {
		go func() {
			panicOrError := infra.SafeRun(func() error {
				// Start the rule which runs async
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
	var rs *rule.RuleState = nil
	var err error = nil
	// Validate the topo
	panicOrError := infra.SafeRun(func() error {
		rs, err = createRuleState(r)
		return err
	})

	if panicOrError != nil { // when recovering rules, assume the rules are valid, so always add it to the registry
		conf.Log.Errorf("Create rule topo error: %v", err)
		r.Triggered = false
		registry.Store(r.Id, rs)
	}
	if !r.Triggered {
		return fmt.Sprintf("Rule %s was stopped.", r.Id)
	} else {
		panicOrError := infra.SafeRun(func() error {
			// Start the rule which runs async
			return rs.Start()
		})
		if panicOrError != nil {
			return fmt.Sprintf("Rule %s start failed: %s", r.Id, panicOrError)
		}
	}
	return fmt.Sprintf("Rule %s was started.", r.Id)
}

// reload password from resources if the config both include password(as fake password) and resourceId
func replacePasswdForConfig(typ string, name string, config map[string]interface{}) map[string]interface{} {
	if r, ok := config["resourceId"]; ok {
		if resourceId, ok := r.(string); ok {
			return meta.ReplacePasswdForConfig(typ, name, resourceId, config)
		}
	}
	return config
}

func replacePasswdByRuleID(ruleId string, actionIndex int, name string, config map[string]interface{}) map[string]interface{} {
	rule, err := ruleProcessor.GetRuleById(ruleId)
	if err != nil {
		return config
	}
	if len(rule.Actions) <= actionIndex {
		return config
	}
	rc, ok := rule.Actions[actionIndex][name]
	if !ok {
		return config
	}
	ruleConfig, ok := rc.(map[string]interface{})
	if !ok {
		return config
	}
	for key := range hidden.GetHiddenKeys() {
		if v, ok := config[key]; ok && v == hidden.PASSWORD {
			config[key] = ruleConfig[key]
			continue
		}
	}
	return config
}

func replaceRulePassword(id, ruleJson string) (string, error) {
	r := &api.Rule{
		Triggered: true,
	}
	if err := json.Unmarshal([]byte(ruleJson), r); err != nil {
		return "", err
	}
	existsRule, err := ruleProcessor.GetRuleById(id)
	if err != nil {
		return "", err
	}

	var replacePassword bool
	for i, action := range r.Actions {
		if i >= len(existsRule.Actions) {
			break
		}
		for k, v := range action {
			if m, ok := v.(map[string]interface{}); ok {
				for key := range hidden.GetHiddenKeys() {
					if v, ok := m[key]; ok && v == hidden.PASSWORD {
						oldAction := existsRule.Actions[i]
						oldV, ok := oldAction[k]
						if ok {
							if oldM, ok := oldV.(map[string]interface{}); ok {
								oldPasswordValue, ok := oldM[key]
								if ok {
									oldPasswordStr, ok := oldPasswordValue.(string)
									if ok && oldPasswordStr != hidden.PASSWORD {
										m[key] = oldPasswordStr
										action[k] = m
										r.Actions[i] = action
										replacePassword = true
										continue
									}
								}
							}
						}
					}
				}
			}
		}
	}
	if !replacePassword {
		return ruleJson, nil
	}
	b, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func updateRule(ruleId, ruleJson string, replacePasswd bool) error {
	// Validate the rule json
	r, err := ruleProcessor.GetRuleByJson(ruleId, ruleJson)
	if err != nil {
		return fmt.Errorf("Invalid rule json: %v", err)
	}
	if replacePasswd {
		for i, action := range r.Actions {
			for k, v := range action {
				if m, ok := v.(map[string]interface{}); ok {
					m = replacePasswdForConfig("sink", k, m)
					action[k] = m
				}
			}
			r.Actions[i] = action
		}
	}
	if rs, ok := registry.Load(r.Id); ok {
		err := rs.UpdateTopo(r)
		if err != nil {
			return err
		}
		_, err = ruleProcessor.ExecReplaceRuleState(rs.RuleId, r.Triggered)
		return err
	} else {
		return fmt.Errorf("Rule %s registry not found, try to delete it and recreate", r.Id)
	}
}

func deleteRule(name string) (result string) {
	if rs, ok := registry.Delete(name); ok {
		rs.Close()
		deleteRuleMetrics(name)
		result = fmt.Sprintf("Rule %s was deleted.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
	}
	return
}

func deleteRuleMetrics(name string) {
	if conf.Config != nil && conf.Config.Basic.Prometheus {
		promMetrics.RemoveRuleStatus(name)
	}
}

func startRule(name string) error {
	return reRunRule(name, false)
}

func startRuleInternal(name string) error {
	return reRunRule(name, true)
}

// reRunRule rerun the rule from optimize to Open the operator in order to refresh the schema
func reRunRule(name string, isInternal bool) error {
	rs, ok := registry.Load(name)
	if !ok {
		return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found in registry, please check if it is created", name))
	} else {
		if !isInternal {
			if rule, err := ruleProcessor.ExecReplaceRuleState(rs.RuleId, true); err != nil {
				return err
			} else {
				rs.Rule = rule
			}
		}
		return rs.UpdateTopo(rs.Rule)
	}
}

func stopRuleInternal(name string) {
	var err error
	if rs, ok := registry.Load(name); ok {
		err = rs.InternalStop()
		if err != nil {
			conf.Log.Warn(err)
		}
	}
}

func stopRule(name string) (result string, err error) {
	if rs, ok := registry.Load(name); ok {
		err = rs.Stop()
		if err != nil {
			conf.Log.Warn(err)
		}
		_, err = ruleProcessor.ExecReplaceRuleState(name, false)
		if err != nil {
			conf.Log.Warnf("stop rule found error: %s", err.Error())
		}
		result = fmt.Sprintf("Rule %s was stopped.", name)
	} else {
		result = fmt.Sprintf("Rule %s was not found.", name)
		err = errorx.NewWithCode(errorx.NOT_FOUND, result)
	}
	return
}

func restartRule(name string) error {
	return reRunRule(name, false)
}

func getAllRuleStatus() (string, error) {
	rules, err := ruleProcessor.GetAllRules()
	if err != nil {
		return "", err
	}
	m := make(map[string]ruleExceptionStatus)
	for _, ruleID := range rules {
		s, err := getRuleExceptionStatus(ruleID)
		if err != nil {
			return "", err
		}
		m[ruleID] = s
	}
	b, _ := json.Marshal(m)
	return string(b), nil
}

func getRuleExceptionStatus(name string) (ruleExceptionStatus, error) {
	s := ruleExceptionStatus{
		lastExceptionTime: -1,
	}
	if rs, ok := registry.Load(name); ok {
		result, err := rs.GetState()
		if err != nil {
			return s, err
		}
		s.Status = result
		if result == rule.RuleStarted {
			keys, values := (*rs.Topology).GetMetrics()
			for i, key := range keys {
				if strings.Contains(key, "last_exception_time") {
					v := values[i].(int64)
					if v > s.lastExceptionTime {
						s.lastExceptionTime = v
						total, last := getTargetException(keys, values, key[:strings.Index(key, "_last_exception_time")])
						s.LastException = last
						s.ExceptionsTotal = total
					}
				}
			}
		}
	}
	return s, nil
}

func getTargetException(keys []string, values []any, prefix string) (int64, string) {
	var t int64
	lastException := ""
	for i, key := range keys {
		if key == fmt.Sprintf("%s_exceptions_total", prefix) {
			t = values[i].(int64)
			continue
		}
		if key == fmt.Sprintf("%s_last_exception", prefix) {
			lastException = values[i].(string)
			continue
		}
	}
	return t, lastException
}

type ruleExceptionStatus struct {
	Status            string `json:"status"`
	LastException     string `json:"last_exception"`
	ExceptionsTotal   int64  `json:"exceptions_total"`
	lastExceptionTime int64
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
			lastStart, lastStop, nextStart := rs.GetScheduleTimestamp()
			metrics += fmt.Sprintf(`"lastStartTimestamp": "%v",`, lastStart)
			metrics += fmt.Sprintf(`"lastStopTimestamp": "%v",`, lastStop)
			metrics += fmt.Sprintf(`"nextStopTimestamp": "%v",`, nextStart)
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
			if err = json.Indent(dst, cast.StringToBytes(metrics), "", "  "); err != nil {
				result = metrics
			} else {
				result = dst.String()
			}
		} else {
			return getStoppedState(result)
		}
		return result, nil
	} else {
		return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
	}
}

func getStoppedState(message string) (string, error) {
	s := map[string]string{
		"status":  "stopped",
		"message": message,
	}
	re, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(re), nil
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

type ruleWrapper struct {
	rule  *api.Rule
	state string
}

func getAllRulesWithState() ([]ruleWrapper, error) {
	ruleIds, err := ruleProcessor.GetAllRules()
	if err != nil {
		return nil, err
	}
	sort.Strings(ruleIds)
	rules := make([]ruleWrapper, 0, len(ruleIds))
	for _, id := range ruleIds {
		rs, ok := registry.Load(id)
		if ok {
			s, _ := rs.GetState()
			rules = append(rules, ruleWrapper{rule: rs.Rule, state: s})
		}
	}
	return rules, nil
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

func validateRule(name, ruleJson string) ([]string, bool, error) {
	// Validate the rule json
	rule, err := ruleProcessor.GetRuleByJson(name, ruleJson)
	if err != nil {
		return nil, false, fmt.Errorf("invalid rule json: %v", err)
	}
	var sources []string
	if len(rule.Sql) > 0 {
		stmt, _ := xsql.GetStatementFromSql(rule.Sql)
		s, err := store.GetKV("stream")
		if err != nil {
			return nil, false, err
		}
		sources = xsql.GetStreams(stmt)
		for _, result := range sources {
			_, err := xsql.GetDataSource(s, result)
			if err != nil {
				return nil, false, err
			}
		}
	} else if rule.Graph != nil {
		tp, err := planner.PlanByGraph(rule)
		if err != nil {
			return nil, false, fmt.Errorf("invalid rule graph: %v", err)
		}
		sources = tp.GetTopo().Sources
	}
	return sources, true, nil
}

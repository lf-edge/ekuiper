package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
)

var registry *RuleRegistry

type RuleState struct {
	Name      string
	Topology  *xstream.TopologyNew
	Triggered bool
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

func (rr *RuleRegistry) Delete(key string) {
	rr.Lock()
	delete(rr.internal, key)
	rr.Unlock()
}

func createRuleState(rule *api.Rule) (*RuleState, error) {
	rs := &RuleState{
		Name: rule.Id,
	}
	registry.Store(rule.Id, rs)
	if tp, err := ruleProcessor.ExecInitRule(rule); err != nil {
		return rs, err
	} else {
		rs.Topology = tp
		rs.Triggered = true
		return rs, nil
	}
}

func doStartRule(rs *RuleState) error {
	rs.Triggered = true
	ruleProcessor.ExecReplaceRuleState(rs.Name, true)
	go func() {
		tp := rs.Topology
		select {
		case err := <-tp.Open():
			tp.GetContext().SetError(err)
			logger.Printf("closing rule %s for error: %v", rs.Name, err)
			tp.Cancel()
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
			return nil, err
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
		return "", fmt.Errorf("Rule %s is not found", name)
	}
}

func doGetRuleState(rs *RuleState) (string, error) {
	result := ""
	if !rs.Triggered {
		result = "Stopped: canceled manually or by error."
		return result, nil
	}
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
		return "", common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("Rule %s is not found", name))
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
	}
	err := doStartRule(rs)
	if err != nil {
		return err
	}
	return nil
}

func stopRule(name string) (result string) {
	if rs, ok := registry.Load(name); ok && rs.Triggered {
		(*rs.Topology).Cancel()
		rs.Triggered = false
		ruleProcessor.ExecReplaceRuleState(name, false)
		result = fmt.Sprintf("Rule %s was stopped.", name)
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

	rs, err := createRuleState(rule)
	if err != nil {
		return fmt.Sprintf("%v", err)
	}

	if rule.Triggered {
		if err = doStartRule(rs); err != nil {
			return fmt.Sprintf("%v", err)
		}
		return fmt.Sprintf("Rule %s was started.", name)
	}
	return stopRule(name)
}

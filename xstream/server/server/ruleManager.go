package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"sync"
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
	if tp, err := ruleProcessor.ExecInitRule(rule); err != nil {
		return nil, err
	} else {
		rs := &RuleState{
			Name:      rule.Id,
			Topology:  tp,
			Triggered: true,
		}
		registry.Store(rule.Id, rs)
		return rs, nil
	}
}

func doStartRule(rs *RuleState) error {
	rs.Triggered = true
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

func getRuleStatus(name string) (string, error) {
	result := ""
	if rs, ok := registry.Load(name); ok {
		if !rs.Triggered {
			result = "Stopped: canceled manually."
			return result, nil
		}
		c := (*rs.Topology).GetContext()
		if c != nil {
			err := c.Err()
			switch err {
			case nil:
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
					result = "Running with metrics:\n" + metrics
				} else {
					result = "Running with metrics:\n" + dst.String()
				}
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
	} else {
		return "", fmt.Errorf("Rule %s is not found", name)
	}
}

func startRule(name string) error {
	var rs *RuleState
	rs, ok := registry.Load(name)
	if !ok {
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
	if rs, ok := registry.Load(name); ok {
		(*rs.Topology).Cancel()
		rs.Triggered = false
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

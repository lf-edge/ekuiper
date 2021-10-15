// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/native"
	"github.com/lf-edge/ekuiper/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/internal/service"
	"github.com/lf-edge/ekuiper/internal/topo/sink"
	"strings"
	"time"
)

const QueryRuleId = "internal-ekuiper_query_rule"

type Server int

func (t *Server) CreateQuery(sql string, reply *string) error {
	if _, ok := registry.Load(QueryRuleId); ok {
		stopQuery()
	}
	tp, err := ruleProcessor.ExecQuery(QueryRuleId, sql)
	if err != nil {
		return err
	} else {
		rs := &RuleState{Name: QueryRuleId, Topology: tp, Triggered: true}
		registry.Store(QueryRuleId, rs)
		msg := fmt.Sprintf("Query was submit successfully.")
		logger.Println(msg)
		*reply = fmt.Sprintf(msg)
	}
	return nil
}

func stopQuery() {
	if rs, ok := registry.Load(QueryRuleId); ok {
		logger.Printf("stop the query.")
		(*rs.Topology).Cancel()
		registry.Delete(QueryRuleId)
	}
}

/**
 * qid is not currently used.
 */
func (t *Server) GetQueryResult(_ string, reply *string) error {
	if rs, ok := registry.Load(QueryRuleId); ok {
		c := (*rs.Topology).GetContext()
		if c != nil && c.Err() != nil {
			return c.Err()
		}
	}

	sink.QR.LastFetch = time.Now()
	sink.QR.Mux.Lock()
	if len(sink.QR.Results) > 0 {
		*reply = strings.Join(sink.QR.Results, "")
		sink.QR.Results = make([]string, 10)
	} else {
		*reply = ""
	}
	sink.QR.Mux.Unlock()
	return nil
}

func (t *Server) Stream(stream string, reply *string) error {
	content, err := streamProcessor.ExecStmt(stream)
	if err != nil {
		return fmt.Errorf("Stream command error: %s", err)
	} else {
		for _, c := range content {
			*reply = *reply + fmt.Sprintln(c)
		}
	}
	return nil
}

func (t *Server) CreateRule(rule *model.RPCArgDesc, reply *string) error {
	r, err := ruleProcessor.ExecCreate(rule.Name, rule.Json)
	if err != nil {
		return fmt.Errorf("Create rule error : %s.", err)
	} else {
		*reply = fmt.Sprintf("Rule %s was created successfully, please use 'bin/kuiper getstatus rule %s' command to get rule status.", rule.Name, rule.Name)
	}
	//Start the rule
	rs, err := createRuleState(r)
	if err != nil {
		return err
	}
	err = doStartRule(rs)
	if err != nil {
		return err
	}
	return nil
}

func (t *Server) GetStatusRule(name string, reply *string) error {
	if r, err := getRuleStatus(name); err != nil {
		return err
	} else {
		*reply = r
	}
	return nil
}

func (t *Server) GetTopoRule(name string, reply *string) error {
	if r, err := getRuleTopo(name); err != nil {
		return err
	} else {
		dst := &bytes.Buffer{}
		if err = json.Indent(dst, []byte(r), "", "  "); err != nil {
			*reply = r
		} else {
			*reply = dst.String()
		}
	}
	return nil
}

func (t *Server) StartRule(name string, reply *string) error {
	if err := startRule(name); err != nil {
		return err
	} else {
		*reply = fmt.Sprintf("Rule %s was started", name)
	}
	return nil
}

func (t *Server) StopRule(name string, reply *string) error {
	*reply = stopRule(name)
	return nil
}

func (t *Server) RestartRule(name string, reply *string) error {
	err := restartRule(name)
	if err != nil {
		return err
	}
	*reply = fmt.Sprintf("Rule %s was restarted.", name)
	return nil
}

func (t *Server) DescRule(name string, reply *string) error {
	r, err := ruleProcessor.ExecDesc(name)
	if err != nil {
		return fmt.Errorf("Desc rule error : %s.", err)
	} else {
		*reply = r
	}
	return nil
}

func (t *Server) ShowRules(_ int, reply *string) error {
	r, err := getAllRulesWithStatus()
	if err != nil {
		return fmt.Errorf("Show rule error : %s.", err)
	}
	if len(r) == 0 {
		*reply = "No rule definitions are found."
	} else {
		result, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("Show rule error : %s.", err)
		}
		dst := &bytes.Buffer{}
		if err := json.Indent(dst, result, "", "  "); err != nil {
			return fmt.Errorf("Show rule error : %s.", err)
		}
		*reply = dst.String()
	}
	return nil
}

func (t *Server) DropRule(name string, reply *string) error {
	deleteRule(name)
	r, err := ruleProcessor.ExecDrop(name)
	if err != nil {
		return fmt.Errorf("Drop rule error : %s.", err)
	} else {
		err := t.StopRule(name, reply)
		if err != nil {
			return err
		}
	}
	*reply = r
	return nil
}

func (t *Server) CreatePlugin(arg *model.PluginDesc, reply *string) error {
	pt := plugin.PluginType(arg.Type)
	p, err := getPluginByJson(arg, pt)
	if err != nil {
		return fmt.Errorf("Create plugin error: %s", err)
	}
	if p.GetFile() == "" {
		return fmt.Errorf("Create plugin error: Missing plugin file url.")
	}
	if pt == plugin.PORTABLE {
		err = portable.GetManager().Register(p)
	} else {
		err = native.GetManager().Register(pt, p)
	}
	if err != nil {
		return fmt.Errorf("Create plugin error: %s", err)
	} else {
		*reply = fmt.Sprintf("Plugin %s is created.", p.GetName())
	}
	return nil
}

func (t *Server) RegisterPlugin(arg *model.PluginDesc, reply *string) error {
	p, err := getPluginByJson(arg, plugin.FUNCTION)
	if err != nil {
		return fmt.Errorf("Register plugin functions error: %s", err)
	}
	if len(p.GetSymbols()) == 0 {
		return fmt.Errorf("Register plugin functions error: Missing function list.")
	}
	err = native.GetManager().RegisterFuncs(p.GetName(), p.GetSymbols())
	if err != nil {
		return fmt.Errorf("Create plugin error: %s", err)
	} else {
		*reply = fmt.Sprintf("Plugin %s is created.", p.GetName())
	}
	return nil
}

func (t *Server) DropPlugin(arg *model.PluginDesc, reply *string) error {
	pt := plugin.PluginType(arg.Type)
	p, err := getPluginByJson(arg, pt)
	if err != nil {
		return fmt.Errorf("Drop plugin error: %s", err)
	}
	if pt == plugin.PORTABLE {
		err = portable.GetManager().Delete(p.GetName())
		if err != nil {
			return fmt.Errorf("Drop plugin error: %s", err)
		} else {
			*reply = fmt.Sprintf("Plugin %s is dropped .", p.GetName())
		}
	} else {
		err = native.GetManager().Delete(pt, p.GetName(), arg.Stop)
		if err != nil {
			return fmt.Errorf("Drop plugin error: %s", err)
		} else {
			if arg.Stop {
				*reply = fmt.Sprintf("Plugin %s is dropped and Kuiper will be stopped.", p.GetName())
			} else {
				*reply = fmt.Sprintf("Plugin %s is dropped and Kuiper must restart for the change to take effect.", p.GetName())
			}
		}
	}
	return nil
}

func (t *Server) ShowPlugins(arg int, reply *string) error {
	pt := plugin.PluginType(arg)
	l := native.GetManager().List(pt)
	if len(l) == 0 {
		l = append(l, "No plugin is found.")
	}
	*reply = strings.Join(l, "\n")
	return nil
}

func (t *Server) ShowUdfs(_ int, reply *string) error {
	l := native.GetManager().ListSymbols()
	if len(l) == 0 {
		l = append(l, "No udf is found.")
	}
	*reply = strings.Join(l, "\n")
	return nil
}

func (t *Server) DescPlugin(arg *model.PluginDesc, reply *string) error {
	pt := plugin.PluginType(arg.Type)
	p, err := getPluginByJson(arg, pt)
	if err != nil {
		return fmt.Errorf("Describe plugin error: %s", err)
	}
	var m interface{}
	var ok bool
	if pt == plugin.PORTABLE {
		m, ok = portable.GetManager().GetPluginInfo(p.GetName())
	} else {
		m, ok = native.GetManager().GetPluginInfo(pt, p.GetName())
	}
	if !ok {
		return fmt.Errorf("Describe plugin error: not found")
	} else {
		r, err := marshalDesc(m)
		if err != nil {
			return fmt.Errorf("Describe plugin error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) DescUdf(arg string, reply *string) error {
	m, ok := native.GetManager().GetPluginBySymbol(plugin.FUNCTION, arg)
	if !ok {
		return fmt.Errorf("Describe udf error: not found")
	} else {
		j := map[string]string{
			"name":   arg,
			"plugin": m,
		}
		r, err := marshalDesc(j)
		if err != nil {
			return fmt.Errorf("Describe udf error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) CreateService(arg *model.RPCArgDesc, reply *string) error {
	sd := &service.ServiceCreationRequest{}
	if arg.Json != "" {
		if err := json.Unmarshal([]byte(arg.Json), sd); err != nil {
			return fmt.Errorf("Parse service %s error : %s.", arg.Json, err)
		}
	}
	if sd.Name != arg.Name {
		return fmt.Errorf("Create service error: name mismatch.")
	}
	if sd.File == "" {
		return fmt.Errorf("Create service error: Missing service file url.")
	}
	err := service.GetManager().Create(sd)
	if err != nil {
		return fmt.Errorf("Create service error: %s", err)
	} else {
		*reply = fmt.Sprintf("Service %s is created.", arg.Name)
	}
	return nil
}

func (t *Server) DescService(name string, reply *string) error {
	s, err := service.GetManager().Get(name)
	if err != nil {
		return fmt.Errorf("Desc service error : %s.", err)
	} else {
		r, err := marshalDesc(s)
		if err != nil {
			return fmt.Errorf("Describe service error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) DescServiceFunc(name string, reply *string) error {
	s, err := service.GetManager().GetFunction(name)
	if err != nil {
		return fmt.Errorf("Desc service func error : %s.", err)
	} else {
		r, err := marshalDesc(s)
		if err != nil {
			return fmt.Errorf("Describe service func error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) DropService(name string, reply *string) error {
	err := service.GetManager().Delete(name)
	if err != nil {
		return fmt.Errorf("Drop service error : %s.", err)
	}
	*reply = fmt.Sprintf("Service %s is dropped", name)
	return nil
}

func (t *Server) ShowServices(_ int, reply *string) error {
	s, err := service.GetManager().List()
	if err != nil {
		return fmt.Errorf("Show service error: %s.", err)
	}
	if len(s) == 0 {
		*reply = "No service definitions are found."
	} else {
		r, err := marshalDesc(s)
		if err != nil {
			return fmt.Errorf("Show service error: %v", err)
		}
		*reply = r
	}
	return nil
}

func (t *Server) ShowServiceFuncs(_ int, reply *string) error {
	s, err := service.GetManager().ListFunctions()
	if err != nil {
		return fmt.Errorf("Show service funcs error: %s.", err)
	}
	if len(s) == 0 {
		*reply = "No service definitions are found."
	} else {
		r, err := marshalDesc(s)
		if err != nil {
			return fmt.Errorf("Show service funcs error: %v", err)
		}
		*reply = r
	}
	return nil
}

func marshalDesc(m interface{}) (string, error) {
	s, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("invalid json %v", m)
	}
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, s, "", "  "); err != nil {
		return "", fmt.Errorf("indent json error %v", err)
	}
	return dst.String(), nil
}

func getPluginByJson(arg *model.PluginDesc, pt plugin.PluginType) (plugin.Plugin, error) {
	p := plugin.NewPluginByType(pt)
	if arg.Json != "" {
		if err := json.Unmarshal([]byte(arg.Json), p); err != nil {
			return nil, fmt.Errorf("Parse plugin %s error : %s.", arg.Json, err)
		}
	}
	p.SetName(arg.Name)
	return p, nil
}

func init() {
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			if registry != nil {
				if _, ok := registry.Load(QueryRuleId); !ok {
					continue
				}

				n := time.Now()
				w := 10 * time.Second
				if v := n.Sub(sink.QR.LastFetch); v >= w {
					logger.Printf("The client seems no longer fetch the query result, stop the query now.")
					stopQuery()
					ticker.Stop()
					return
				}
			}
		}
	}()
}

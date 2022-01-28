// Copyright 2022 EMQ Technologies Co., Ltd.
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

//go:build rpc || !core
// +build rpc !core

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/model"
	"github.com/lf-edge/ekuiper/internal/topo/sink"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

const QueryRuleId = "internal-ekuiper_query_rule"

func init() {
	servers["rpc"] = rpcComp{}
}

type rpcComp struct {
	s *http.Server
}

func (r rpcComp) register() {}

func (r rpcComp) serve() {
	// Start rpc service
	server := new(Server)
	portRpc := conf.Config.Basic.Port
	ipRpc := conf.Config.Basic.Ip
	rpcSrv := rpc.NewServer()
	err := rpcSrv.Register(server)
	if err != nil {
		logger.Fatal("Format of service Server isn'restHttpType correct. ", err)
	}
	srvRpc := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", ipRpc, portRpc),
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      rpcSrv,
	}
	r.s = srvRpc
	go func() {
		if err = srvRpc.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Error serving rpc service:", err)
		}
	}()
}

func (r rpcComp) close() {
	if r.s != nil {
		if err := r.s.Shutdown(context.TODO()); err != nil {
			logger.Errorf("rpc server shutdown error: %v", err)
		}
		logger.Info("rpc server shutdown.")
	}
}

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
		*reply = strings.Join(sink.QR.Results, "\n")
		sink.QR.Results = make([]string, 0, 10)
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
				}
			}
		}
	}()
}

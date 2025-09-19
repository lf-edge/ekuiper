// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/io/sink"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/model"
	"github.com/lf-edge/ekuiper/v2/internal/topo/rule"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

const QueryRuleId = "internal-ekuiper_query_rule"

func init() {
	servers["rpc"] = &rpcComp{}
}

type rpcComp struct {
	s *http.Server
}

func (r *rpcComp) register() {}

func (r *rpcComp) serve() {
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
		Addr:         cast.JoinHostPortInt(ipRpc, portRpc),
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
	initQuery()
}

func (r *rpcComp) close() {
	if r.s != nil {
		if err := r.s.Shutdown(context.TODO()); err != nil {
			logger.Errorf("rpc server shutdown error: %v", err)
		}
		logger.Info("rpc server shutdown.")
	}
}

type Server int

func (t *Server) CreateQuery(sql string, reply *string) error {
	stopQuery()
	logger.Printf("stop the previous query if exists.")
	tempRule := def.GetDefaultRule(QueryRuleId, sql)
	tempRule.Temp = true
	tempRule.Triggered = true
	tempRule.Actions = []map[string]any{
		{
			"logToMemory": map[string]any{},
		},
	}
	ruleJson, _ := json.Marshal(tempRule)
	_, err := registry.CreateRule(QueryRuleId, string(ruleJson))
	if err != nil {
		return err
	} else {
		msg := fmt.Sprintf("Query was submit successfully.")
		logger.Println(msg)
		*reply = fmt.Sprint(msg)
	}
	return nil
}

func stopQuery() {
	if rs, ok := registry.load(QueryRuleId); ok {
		logger.Printf("stop the query.")
		_ = rs.Delete()
	}
}

func (t *Server) GetQueryResult(_ string, reply *string) error {
	if rs, ok := registry.load(QueryRuleId); ok {
		st := rs.GetState()
		if st == rule.Stopped || st == rule.StoppedByErr {
			return fmt.Errorf("query rule is stopped: %s", rs.GetLastWill())
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
	id, err := registry.CreateRule(rule.Name, rule.Json)
	if err != nil {
		return fmt.Errorf("Create rule %s error : %s.", id, err)
	} else {
		*reply = fmt.Sprintf("Rule %s was created successfully, please use 'bin/kuiper getstatus rule %s' command to get rule status.", rule.Name, rule.Name)
	}
	return nil
}

func (t *Server) GetStatusRule(name string, reply *string) error {
	if r, err := registry.GetRuleStatus(name); err != nil {
		return err
	} else {
		*reply = r
	}
	return nil
}

func (t *Server) GetTopoRule(name string, reply *string) error {
	if r, err := registry.GetRuleTopo(name); err != nil {
		return err
	} else {
		dst := &bytes.Buffer{}
		if err = json.Indent(dst, cast.StringToBytes(r), "", "  "); err != nil {
			*reply = r
		} else {
			*reply = dst.String()
		}
	}
	return nil
}

func (t *Server) StartRule(name string, reply *string) error {
	if err := registry.StartRule(name); err != nil {
		return err
	} else {
		*reply = fmt.Sprintf("Rule %s was started", name)
	}
	return nil
}

func (t *Server) StopRule(name string, reply *string) error {
	if err := registry.StopRule(name); err != nil {
		return err
	} else {
		*reply = fmt.Sprintf("Rule %s was stopped.", name)
	}
	return nil
}

func (t *Server) RestartRule(name string, reply *string) error {
	err := registry.RestartRule(name)
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
	r, err := registry.GetAllRulesWithStatus()
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
	err := registry.DeleteRule(name)
	if err != nil {
		return fmt.Errorf("Drop rule error : %s.", err)
	}
	*reply = fmt.Sprintf("Rule %s is dropped.", name)
	return nil
}

func (t *Server) ValidateRule(rule *model.RPCArgDesc, reply *string) error {
	_, s, err := registry.ValidateRule(rule.Name, rule.Json)
	if s {
		*reply = "The rule has been successfully validated and is confirmed to be correct."
	} else {
		*reply = err.Error()
	}
	return nil
}

func (t *Server) Import(file string, reply *string) error {
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("fail to read file %s: %v", file, err)
	}
	defer f.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)
	if err != nil {
		return fmt.Errorf("fail to convert file %s: %v", file, err)
	}
	content := buf.Bytes()
	rules, counts, err := rulesetProcessor.Import(content)
	if err != nil {
		return fmt.Errorf("import ruleset error: %v", err)
	}
	infra.SafeRun(func() error {
		for _, name := range rules {
			rul, ee := ruleProcessor.GetRuleById(name)
			if ee != nil {
				logger.Error(ee)
				continue
			}
			reply := registry.RecoverRule(rul)
			if reply != "" {
				logger.Error(reply)
			}
		}
		return nil
	})
	*reply = fmt.Sprintf("imported %d streams, %d tables and %d rules", counts[0], counts[1], counts[2])
	return nil
}

func (t *Server) Export(file string, reply *string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	exported, counts, err := rulesetProcessor.Export()
	if err != nil {
		return err
	}
	_, err = io.Copy(f, exported)
	if err != nil {
		return fmt.Errorf("fail to save to file %s:%v", file, err)
	}
	*reply = fmt.Sprintf("exported %d streams, %d tables and %d rules", counts[0], counts[1], counts[2])
	return nil
}

func (t *Server) ImportConfiguration(arg *model.ImportDataDesc, reply *string) error {
	file := arg.FileName
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("fail to read file %s: %v", file, err)
	}
	defer f.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)
	if err != nil {
		return fmt.Errorf("fail to convert file %s: %v", file, err)
	}
	content := buf.Bytes()
	partial := arg.Partial

	var result ImportConfigurationStatus
	if !partial {
		configurationReset()
		result = configurationImport(context.Background(), content, arg.Stop)
	} else {
		result = configurationPartialImport(context.Background(), content)
	}
	marshal, _ := json.Marshal(result)

	dst := &bytes.Buffer{}
	if err := json.Indent(dst, marshal, "", "  "); err != nil {
		return fmt.Errorf("import configuration error: %v", err)
	}
	*reply = dst.String()

	return nil
}

func (t *Server) GetStatusImport(_ int, reply *string) error {
	jsonRsp := configurationStatusExport()
	result, err := json.Marshal(jsonRsp)
	if err != nil {
		return fmt.Errorf("Show rule error : %s.", err)
	}
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, result, "", "  "); err != nil {
		return fmt.Errorf("Show rule error : %s.", err)
	}
	*reply = dst.String()

	return nil
}

func (t *Server) ExportConfiguration(arg *model.ExportDataDesc, reply *string) error {
	rules := arg.Rules
	file := arg.FileName
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	var jsonBytes []byte
	// do not specify rules, export all
	if len(rules) == 0 {
		jsonBytes, err = configurationExport()
	} else {
		jsonBytes, err = ruleMigrationProcessor.ConfigurationPartialExport(rules)
	}
	if err != nil {
		return err
	}
	_, err = io.Copy(f, bytes.NewReader(jsonBytes))
	if err != nil {
		return fmt.Errorf("fail to save to file %s:%v", file, err)
	}
	*reply = fmt.Sprintf("export configuration success")
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

func initQuery() {
	ticker := time.NewTicker(time.Second * 5)
	go infra.SafeRun(func() error {
		for {
			<-ticker.C
			if registry != nil {
				if _, ok := registry.load(QueryRuleId); !ok {
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
	})
}

package server

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/sinks"
	"strings"
	"time"
)

const QUERY_RULE_ID = "internal-xstream_query_rule"

type Server int

func (t *Server) CreateQuery(sql string, reply *string) error {
	if _, ok := registry[QUERY_RULE_ID]; ok {
		stopQuery()
	}
	tp, err := ruleProcessor.ExecQuery(QUERY_RULE_ID, sql)
	if err != nil {
		return err
	} else {
		rs := &RuleState{Name: QUERY_RULE_ID, Topology: tp, Triggered: true}
		registry[QUERY_RULE_ID] = rs
		msg := fmt.Sprintf("Query was submit successfully.")
		logger.Println(msg)
		*reply = fmt.Sprintf(msg)
	}
	return nil
}

func stopQuery() {
	if rs, ok := registry[QUERY_RULE_ID]; ok {
		logger.Printf("stop the query.")
		(*rs.Topology).Cancel()
		delete(registry, QUERY_RULE_ID)
	}
}

/**
 * qid is not currently used.
 */
func (t *Server) GetQueryResult(qid string, reply *string) error {
	if rs, ok := registry[QUERY_RULE_ID]; ok {
		c := (*rs.Topology).GetContext()
		if c != nil && c.Err() != nil {
			return c.Err()
		}
	}

	sinks.QR.LastFetch = time.Now()
	sinks.QR.Mux.Lock()
	if len(sinks.QR.Results) > 0 {
		*reply = strings.Join(sinks.QR.Results, "")
		sinks.QR.Results = make([]string, 10)
	} else {
		*reply = ""
	}
	sinks.QR.Mux.Unlock()
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

func (t *Server) CreateRule(rule *common.Rule, reply *string) error {
	r, err := ruleProcessor.ExecCreate(rule.Name, rule.Json)
	if err != nil {
		return fmt.Errorf("Create rule error : %s.", err)
	} else {
		*reply = fmt.Sprintf("Rule %s was created, please use 'cli getstatus rule $rule_name' command to get rule status.", rule.Name)
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
	r, err := ruleProcessor.ExecShow()
	if err != nil {
		return fmt.Errorf("Show rule error : %s.", err)
	} else {
		*reply = r
	}
	return nil
}

func (t *Server) DropRule(name string, reply *string) error {
	stopRule(name)
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

func init() {
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			if _, ok := registry[QUERY_RULE_ID]; !ok {
				continue
			}

			n := time.Now()
			w := 10 * time.Second
			if v := n.Sub(sinks.QR.LastFetch); v >= w {
				logger.Printf("The client seems no longer fetch the query result, stop the query now.")
				stopQuery()
				ticker.Stop()
				return
			}
		}
	}()
}

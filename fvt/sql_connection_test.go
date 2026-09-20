// Copyright 2026 EMQ Technologies Co., Ltd.
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

package fvt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	mysqlsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/suite"
)

type SQLConnectionRegressionTestSuite struct {
	suite.Suite
}

func TestSQLConnectionRegressionSuite(t *testing.T) {
	suite.Run(t, new(SQLConnectionRegressionTestSuite))
}

// TestIssue1227 verifies that a named SQL connection keeps retrying after the
// database is unavailable during its first dial.
func (s *SQLConnectionRegressionTestSuite) TestIssue1227NamedConnectionRecovers() {
	port := freeTCPPort(s.T())
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	connectionID := "fvt-sql-1227-" + suffix
	dbURL := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)

	resp, err := client.Post("connections", fmt.Sprintf(`{
		"id": %q,
		"typ": "sql",
		"props": {"dburl": %q}
	}`, connectionID, dbURL))
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	_, _ = GetResponseText(resp)
	s.T().Cleanup(func() {
		deleteFVTResource(s.T(), "connections/"+connectionID)
	})

	// Let the database remain unavailable beyond the old 10-second initial
	// connection retry limit before bringing it up.
	time.Sleep(11 * time.Second)
	db, err := setupEmbeddedMysqlServer("127.0.0.1", port)
	s.Require().NoError(err)
	defer db.Close()

	waitForFVTNamed(s.T(), 8*time.Second, "named connection recovery", func() bool {
		status, ok := getConnectionStatus(s.T(), connectionID)
		return ok && status == "connected"
	})
}

// TestIssue1233 verifies that stopping SQL source rules cancels an in-flight
// reconnect and that the rules can be started again after the database returns.
func (s *SQLConnectionRegressionTestSuite) TestIssue1233StopAndRestartSQLRules() {
	port := freeTCPPort(s.T())
	backendPort := freeTCPPort(s.T())
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	connectionID := "fvt-sql-1233-" + suffix
	confKey := "fvt-sql-1233-conf-" + suffix
	simConfKey := "fvt-sql-1233-sim-conf-" + suffix
	streamName := "fvt_sql_1233_stream_" + suffix
	simStreamName := "fvt_sql_1233_sim_stream_" + suffix
	ruleIDs := []string{
		"fvt_sql_1233_rule_a_" + suffix,
		"fvt_sql_1233_rule_b_" + suffix,
	}
	sinkRuleIDs := []string{
		"fvt_sql_1233_sink_rule_a_" + suffix,
	}
	allRuleIDs := append(append([]string{}, ruleIDs...), sinkRuleIDs...)
	dbURL := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)

	db, err := setupEmbeddedMysqlServer("127.0.0.1", backendPort)
	s.Require().NoError(err)
	defer db.Close()
	proxy, err := newTCPProxy(port, backendPort)
	s.Require().NoError(err)
	defer proxy.Close()

	resp, err := client.Post("connections", fmt.Sprintf(`{
		"id": %q,
		"typ": "sql",
		"props": {"dburl": %q}
	}`, connectionID, dbURL))
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	_, _ = GetResponseText(resp)
	waitForFVTNamed(s.T(), 8*time.Second, "initial named connection", func() bool {
		status, ok := getConnectionStatus(s.T(), connectionID)
		return ok && status == "connected"
	})

	conf := map[string]any{
		"interval":           "1s",
		"connectionSelector": connectionID,
		"templateSqlQueryCfg": map[string]any{
			"templateSql": "select a,b from t",
		},
	}
	resp, err = client.CreateConf("sources/sql/confKeys/"+confKey, conf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	_, _ = GetResponseText(resp)
	resp, err = client.CreateConf("sources/simulator/confKeys/"+simConfKey, map[string]any{
		"data":     []map[string]any{{"a": 1, "b": 2}},
		"interval": "100ms",
		"loop":     true,
	})
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	_, _ = GetResponseText(resp)

	resp, err = client.CreateStream(fmt.Sprintf(`{
		"sql": "create stream %s () WITH (TYPE=\"sql\", CONF_KEY=\"%s\", FORMAT=\"json\")"
	}`, streamName, confKey))
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	_, _ = GetResponseText(resp)
	resp, err = client.CreateStream(fmt.Sprintf(`{
		"sql": "create stream %s () WITH (TYPE=\"simulator\", CONF_KEY=\"%s\", FORMAT=\"json\")"
	}`, simStreamName, simConfKey))
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	_, _ = GetResponseText(resp)
	for _, ruleID := range ruleIDs {
		resp, err = client.CreateRule(fmt.Sprintf(`{
			"id": %q,
			"sql": "SELECT * FROM %s",
			"actions": [{"log": {}}]
		}`, ruleID, streamName))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
		_, _ = GetResponseText(resp)
	}
	for _, ruleID := range sinkRuleIDs {
		resp, err = client.CreateRule(fmt.Sprintf(`{
			"id": %q,
			"sql": "SELECT * FROM %s",
			"actions": [{"sql": {"dburl": %q, "connectionSelector": %q, "table": "t", "fields": ["a", "b"]}}]
		}`, ruleID, simStreamName, dbURL, connectionID))
		s.Require().NoError(err)
		body, _ := GetResponseText(resp)
		s.Require().Equal(http.StatusCreated, resp.StatusCode, body)
	}
	for _, ruleID := range ruleIDs {
		waitForFVTNamed(s.T(), 8*time.Second, "initial source rule "+ruleID, func() bool {
			return getRuleStatus(s.T(), ruleID) == "running"
		})
	}
	for _, ruleID := range sinkRuleIDs {
		waitForFVTNamed(s.T(), 8*time.Second, "initial sink rule "+ruleID, func() bool {
			return getRuleStatus(s.T(), ruleID) == "running"
		})
	}
	waitForFVTNamed(s.T(), 8*time.Second, "initial source output", func() bool {
		return streamLogContains(ruleIDs[0], "sink result", `\"b\":2`) && streamLogContains(ruleIDs[1], "sink result", `\"b\":2`)
	})
	initialResults := map[string]int{
		ruleIDs[0]: streamLogCount(ruleIDs[0], "sink result", `\"b\":2`),
		ruleIDs[1]: streamLogCount(ruleIDs[1], "sink result", `\"b\":2`),
	}
	// Switch the public proxy into a TCP blackhole without changing the public
	// listener. This models a firewall DROP without a close/rebind race.
	s.Require().Greater(proxy.ActiveConnections(), 0, "the SQL source must have an active database session")
	// Snapshot the backend sessions before closing the proxy connections. The
	// MySQL server removes sessions asynchronously after their sockets close,
	// so enumerating them after Block can race with that cleanup.
	var sessionIDs []uint32
	s.Require().NoError(db.SessionManager().Iter(func(session mysqlsql.Session) (bool, error) {
		sessionIDs = append(sessionIDs, session.ID())
		return false, nil
	}))
	s.Require().NotEmpty(sessionIDs, "the SQL source must have an active database session")
	s.Require().NoError(proxy.Block())
	s.Require().NoError(db.Close())
	for _, sessionID := range sessionIDs {
		s.Require().NoError(db.SessionManager().KillConnection(sessionID))
	}
	blackholeStarted := time.Now()
	waitForFVTNamed(s.T(), 8*time.Second, "blocked reconnect", func() bool {
		// The three rules share one *sql.DB and its connection mutex, so the
		// driver may serialize their reconnects. One accepted session proves the
		// shared SQL operation has entered the outage path.
		return proxy.ActiveConnections() > 0
	})
	s.T().Logf("blackhole accepted %d connections after %s", proxy.ActiveConnections(), time.Since(blackholeStarted))
	// Give both source goroutines time to enter the blocked database operation
	// before stopping the rules. The source interval is one second, and the
	// first failed pull sets needReconnect before the next pull blocks in
	// Reconnect.
	time.Sleep(2 * time.Second)

	stopResults := make(chan error, len(allRuleIDs))
	for _, ruleID := range allRuleIDs {
		go func(id string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			status, _, err := doFVTRequest(ctx, http.MethodPost, "rules/"+id+"/stop", "")
			if err != nil {
				stopResults <- err
				return
			}
			if status != http.StatusOK {
				stopResults <- fmt.Errorf("stop rule %s returned HTTP %d", id, status)
				return
			}
			stopResults <- nil
		}(ruleID)
	}
	for range allRuleIDs {
		s.Require().NoError(<-stopResults)
	}

	db, err = setupEmbeddedMysqlServer("127.0.0.1", backendPort)
	s.Require().NoError(err)
	defer db.Close()
	s.Require().NoError(proxy.Unblock())

	startResults := make(chan error, len(allRuleIDs))
	for _, ruleID := range allRuleIDs {
		go func(id string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			status, _, err := doFVTRequest(ctx, http.MethodPost, "rules/"+id+"/start", "")
			cancel()
			if err != nil {
				startResults <- err
				return
			}
			if status != http.StatusOK {
				startResults <- fmt.Errorf("start rule %s returned HTTP %d", id, status)
				return
			}
			startResults <- nil
		}(ruleID)
	}
	for range allRuleIDs {
		s.Require().NoError(<-startResults)
	}
	for _, ruleID := range allRuleIDs {
		waitForFVTNamed(s.T(), 8*time.Second, "restarted rule "+ruleID, func() bool {
			return getRuleStatus(s.T(), ruleID) == "running"
		})
	}
	for _, ruleID := range ruleIDs {
		waitForFVTNamed(s.T(), 8*time.Second, "post-restart source output "+ruleID, func() bool {
			return streamLogCount(ruleID, "sink result", `\"b\":2`) > initialResults[ruleID]
		})
	}

	for _, ruleID := range allRuleIDs {
		deleteFVTResource(s.T(), "rules/"+ruleID)
	}
	deleteFVTResource(s.T(), "streams/"+streamName)
	deleteFVTResource(s.T(), "streams/"+simStreamName)
	deleteFVTResource(s.T(), "connections/"+connectionID)
}

// TestIssue1236 verifies that a failed SQL lookup creation has a finite
// lifetime, releases the lookup lock, and does not prevent a later lookup or
// the lookup listing API from succeeding.
func (s *SQLConnectionRegressionTestSuite) TestIssue1236FailedLookupReleasesAPI() {
	port := freeTCPPort(s.T())
	badPort := freeTCPPort(s.T())
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	goodConnectionID := "fvt-sql-1236-good-" + suffix
	badConnectionID := "fvt-sql-1236-bad-" + suffix
	goodConfKey := "fvt-sql-1236-good-conf-" + suffix
	badConfKey := "fvt-sql-1236-bad-conf-" + suffix
	goodLookup := "fvt_sql_1236_good_" + suffix
	secondLookup := "fvt_sql_1236_second_" + suffix
	badLookup := "fvt_sql_1236_bad_" + suffix
	goodURL := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)
	badURL := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", badPort)

	db, err := setupEmbeddedMysqlServer("127.0.0.1", port)
	s.Require().NoError(err)
	defer db.Close()

	createSQLConnection := func(id, dbURL string) {
		resp, e := client.Post("connections", fmt.Sprintf(`{
			"id": %q,
			"typ": "sql",
			"props": {"dburl": %q}
		}`, id, dbURL))
		s.Require().NoError(e)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
		_, _ = GetResponseText(resp)
	}
	createSQLConnection(goodConnectionID, goodURL)
	createSQLConnection(badConnectionID, badURL)

	goodConf := map[string]any{"connectionSelector": goodConnectionID}
	badConf := map[string]any{"connectionSelector": badConnectionID}
	resp, err := client.CreateConf("sources/sql/confKeys/"+goodConfKey, goodConf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	_, _ = GetResponseText(resp)
	resp, err = client.CreateConf("sources/sql/confKeys/"+badConfKey, badConf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	_, _ = GetResponseText(resp)

	createLookup := func(name, confKey string) (int, []byte, error) {
		statement := fmt.Sprintf(`{"sql":"CREATE TABLE %s() WITH (DATASOURCE=\"t\", CONF_KEY=\"%s\", TYPE=\"sql\", KIND=\"lookup\", KEY=\"a\")"}`, name, confKey)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return doFVTRequest(ctx, http.MethodPost, "tables", statement)
	}

	status, body, err := createLookup(goodLookup, goodConfKey)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, status, string(body))

	badResult := make(chan fvtHTTPResult, 1)
	started := time.Now()
	go func() {
		status, body, err := createLookup(badLookup, badConfKey)
		badResult <- fvtHTTPResult{status: status, body: body, err: err}
	}()

	// The bad create holds the lookup lock while it waits for the named
	// connection. The list request must not remain blocked after the bounded
	// lookup wait expires.
	time.Sleep(300 * time.Millisecond)
	listCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	listStarted := time.Now()
	listResult := make(chan fvtHTTPResult, 1)
	go func() {
		status, body, err := doFVTRequest(listCtx, http.MethodGet, "tables?kind=lookup", "")
		listResult <- fvtHTTPResult{status: status, body: body, err: err}
	}()

	bad := <-badResult
	list := <-listResult
	cancel()
	s.Require().NoError(bad.err)
	s.Require().Equal(http.StatusBadRequest, bad.status, string(bad.body))
	s.Require().Less(time.Since(started), 15*time.Second)
	s.Require().NoError(list.err)
	s.Require().Equal(http.StatusOK, list.status, string(list.body))
	s.Require().Less(time.Since(listStarted), 15*time.Second)
	s.Require().Contains(string(list.body), goodLookup)

	status, body, err = createLookup(secondLookup, goodConfKey)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, status, string(body))

	for _, name := range []string{secondLookup, goodLookup, badLookup} {
		deleteFVTResource(s.T(), "tables/"+name)
	}
	deleteFVTResource(s.T(), "connections/"+goodConnectionID)
	deleteFVTResource(s.T(), "connections/"+badConnectionID)
}

type fvtHTTPResult struct {
	status int
	body   []byte
	err    error
}

func freeTCPPort(t *testing.T) int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func doFVTRequest(ctx context.Context, method, command, body string) (int, []byte, error) {
	commandURL, err := url.Parse(command)
	if err != nil {
		return 0, nil, err
	}
	u := *client.baseUrl
	u.Path = path.Join(u.Path, commandURL.Path)
	u.RawQuery = commandURL.RawQuery

	request, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewBufferString(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", ContentTypeJson)
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	return response.StatusCode, responseBody, err
}

func getConnectionStatus(t *testing.T, id string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status, body, err := doFVTRequest(ctx, http.MethodGet, "connections/"+id, "")
	if err != nil || status != http.StatusOK {
		return "", false
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Logf("decode connection status response: %v; body=%s", err, body)
		return "", false
	}
	value, _ := result["status"].(string)
	return value, true
}

func getRuleStatus(t *testing.T, id string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status, body, err := doFVTRequest(ctx, http.MethodGet, "rules/"+id+"/status", "")
	if err != nil || status != http.StatusOK {
		return ""
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Logf("decode rule status response: %v; body=%s", err, body)
		return ""
	}
	value, _ := result["status"].(string)
	return value
}

func waitForFVTNamed(t *testing.T, timeout time.Duration, name string, condition func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(ConstantInterval)
	}
	t.Fatalf("%s condition was not satisfied before timeout", name)
}

func streamLogContains(parts ...string) bool {
	content, err := os.ReadFile(path.Join(PWD, "log", "stream.log"))
	if err != nil {
		return false
	}
	text := string(content)
	for _, part := range parts {
		if !strings.Contains(text, part) {
			return false
		}
	}
	return true
}

func streamLogCount(parts ...string) int {
	content, err := os.ReadFile(path.Join(PWD, "log", "stream.log"))
	if err != nil {
		return 0
	}
	count := 0
	for _, line := range strings.Split(string(content), "\n") {
		matched := true
		for _, part := range parts {
			if !strings.Contains(line, part) {
				matched = false
				break
			}
		}
		if matched {
			count++
		}
	}
	return count
}

func deleteFVTResource(t *testing.T, resource string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	status, body, err := doFVTRequest(ctx, http.MethodDelete, resource, "")
	if err != nil || (status != http.StatusOK && status != http.StatusNotFound) {
		t.Logf("cleanup %s failed: status=%d err=%v body=%s", resource, status, err, body)
	}
}

type tcpProxy struct {
	listener    net.Listener
	backendAddr string
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	sessions    map[net.Conn]struct{}
	blocked     bool
	blockDone   chan struct{}
	once        sync.Once
}

func newTCPProxy(port, backendPort int) (*tcpProxy, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	proxy := &tcpProxy{
		listener:    listener,
		backendAddr: fmt.Sprintf("127.0.0.1:%d", backendPort),
		connections: make(map[net.Conn]struct{}),
		sessions:    make(map[net.Conn]struct{}),
	}
	go proxy.accept()
	return proxy, nil
}

func (p *tcpProxy) accept() {
	for {
		clientConn, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.sessions[clientConn] = struct{}{}
		if p.blocked {
			blockDone := p.blockDone
			p.connections[clientConn] = struct{}{}
			p.mu.Unlock()
			go func() {
				<-blockDone
				_ = clientConn.Close()
				p.removeConnection(clientConn)
				p.removeSession(clientConn)
			}()
			continue
		}
		p.mu.Unlock()
		backendConn, err := net.Dial("tcp", p.backendAddr)
		if err != nil {
			_ = clientConn.Close()
			p.removeSession(clientConn)
			continue
		}
		p.addConnection(clientConn)
		p.addConnection(backendConn)
		go p.forward(clientConn, backendConn)
		go p.forward(backendConn, clientConn)
	}
}

func (p *tcpProxy) addConnection(conn net.Conn) {
	p.mu.Lock()
	p.connections[conn] = struct{}{}
	p.mu.Unlock()
}

func (p *tcpProxy) removeConnection(conn net.Conn) {
	p.mu.Lock()
	delete(p.connections, conn)
	p.mu.Unlock()
}

func (p *tcpProxy) removeSession(conn net.Conn) {
	p.mu.Lock()
	delete(p.sessions, conn)
	p.mu.Unlock()
}

func (p *tcpProxy) forward(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.removeConnection(dst)
	p.removeConnection(src)
	p.removeSession(src)
}

func (p *tcpProxy) ActiveConnections() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sessions)
}

// Block keeps the public listener accepting TCP connections but does not
// complete the backend connection, modelling a firewall DROP.
func (p *tcpProxy) Block() error {
	p.mu.Lock()
	if p.blocked {
		p.mu.Unlock()
		return nil
	}
	p.blocked = true
	p.blockDone = make(chan struct{})
	p.mu.Unlock()
	return p.CloseConnections()
}

func (p *tcpProxy) Unblock() error {
	p.mu.Lock()
	if !p.blocked {
		p.mu.Unlock()
		return nil
	}
	p.blocked = false
	close(p.blockDone)
	p.blockDone = nil
	p.mu.Unlock()
	return nil
}

func (p *tcpProxy) CloseConnections() error {
	p.mu.Lock()
	connections := make([]net.Conn, 0, len(p.connections))
	for conn := range p.connections {
		connections = append(connections, conn)
	}
	p.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
	return nil
}

func (p *tcpProxy) Close() error {
	var err error
	p.once.Do(func() {
		err = p.listener.Close()
		p.mu.Lock()
		if p.blocked {
			p.blocked = false
			close(p.blockDone)
			p.blockDone = nil
		}
		p.mu.Unlock()
		if closeErr := p.CloseConnections(); err == nil {
			err = closeErr
		}
	})
	return err
}

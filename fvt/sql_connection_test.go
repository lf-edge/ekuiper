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

// sqlFVTTimeout is deliberately generous: this suite runs under -race with an
// embedded MySQL server plus a TCP proxy, so rule startup + first poll can
// take much longer on a loaded CI runner than on a dev machine. The previous
// 8s waits flaked in CI (initial source output timeout).
const sqlFVTTimeout = 30 * time.Second

// sqlFVTRequestTimeout bounds one rule stop/start REST request. Stop/start
// attach or detach a pooled connection, which needs the Manager lock. While
// the database is blackholed, a concurrent patrol health Ping can hold that
// lock for up to one bounded SQL attempt (defaultAttemptTimeout, 10s); that
// is pre-existing lock/status debt tracked for A2. The bound stays finite on
// purpose: without the intended cancellation the reconnect loop never exits,
// so the request would still time out and fail the test.
const sqlFVTRequestTimeout = 20 * time.Second

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

	waitForFVTNamed(s.T(), sqlFVTTimeout, "named connection recovery", func() bool {
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
	waitForFVTNamed(s.T(), sqlFVTTimeout, "initial named connection", func() bool {
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
		// Use distinct PK values so repeated sink inserts do not hit
		// duplicate-key errors on the composite (a,b) primary key and
		// spam the shared *sql.DB with failing writes.
		"data": []map[string]any{
			{"a": 11, "b": 2},
			{"a": 12, "b": 2},
			{"a": 13, "b": 2},
			{"a": 14, "b": 2},
			{"a": 15, "b": 2},
		},
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
		waitForFVTNamed(s.T(), sqlFVTTimeout, "initial source rule "+ruleID, func() bool {
			return getRuleStatus(s.T(), ruleID) == "running"
		})
	}
	for _, ruleID := range sinkRuleIDs {
		waitForFVTNamed(s.T(), sqlFVTTimeout, "initial sink rule "+ruleID, func() bool {
			return getRuleStatus(s.T(), ruleID) == "running"
		})
	}
	// Readiness via per-rule metrics (not the shared stream.log, which is
	// missing in the full CI run). sinkRecordsOut>0 proves the SQL source
	// polled and the log sink received rows. No direct DB queries here:
	// extra concurrent sessions trip data races inside the test-only
	// embedded go-mysql-server under -race.
	waitForSQLSourceOutput(s.T(), sqlFVTTimeout, "initial source output", connectionID, allRuleIDs, nil, func() bool {
		return sinkRecordsOut(s.T(), ruleIDs[0]) > 0 && sinkRecordsOut(s.T(), ruleIDs[1]) > 0
	})
	s.T().Logf("initial source output seen: sinkOut=%v/%v sinkRule=%v",
		sinkRecordsOut(s.T(), ruleIDs[0]), sinkRecordsOut(s.T(), ruleIDs[1]),
		sinkRecordsOut(s.T(), sinkRuleIDs[0]))
	initialSinkOut := map[string]float64{
		ruleIDs[0]: sinkRecordsOut(s.T(), ruleIDs[0]),
		ruleIDs[1]: sinkRecordsOut(s.T(), ruleIDs[1]),
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
	waitForFVTNamed(s.T(), sqlFVTTimeout, "blocked reconnect", func() bool {
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
			ctx, cancel := context.WithTimeout(context.Background(), sqlFVTRequestTimeout)
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
			ctx, cancel := context.WithTimeout(context.Background(), sqlFVTRequestTimeout)
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
		waitForFVTNamed(s.T(), sqlFVTTimeout, "restarted rule "+ruleID, func() bool {
			return getRuleStatus(s.T(), ruleID) == "running"
		})
	}
	for _, ruleID := range ruleIDs {
		waitForFVTNamed(s.T(), sqlFVTTimeout, "post-restart source output "+ruleID, func() bool {
			return sinkRecordsOut(s.T(), ruleID) > initialSinkOut[ruleID]
		})
	}

	for _, ruleID := range allRuleIDs {
		deleteFVTResource(s.T(), "rules/"+ruleID)
	}
	deleteFVTResource(s.T(), "streams/"+streamName)
	deleteFVTResource(s.T(), "streams/"+simStreamName)
	deleteFVTResource(s.T(), "connections/"+connectionID)
}

// TestIssue1236 verifies the lookup CREATE semantics: only static
// configuration errors fail table creation, while an unreachable database
// (TCP blackhole) never blocks the lookup APIs. This guards the original
// regression, where a bad lookup held the global lookup lock while waiting
// for the pooled connection.
func (s *SQLConnectionRegressionTestSuite) TestIssue1236UnreachableLookupCreates() {
	port := freeTCPPort(s.T())
	badPort := freeTCPPort(s.T())
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	goodConnectionID := "fvt-sql-1236-good-" + suffix
	badConnectionID := "fvt-sql-1236-bad-" + suffix
	goodConfKey := "fvt-sql-1236-good-conf-" + suffix
	badConfKey := "fvt-sql-1236-bad-conf-" + suffix
	malformedConfKey := "fvt-sql-1236-malformed-conf-" + suffix
	goodLookup := "fvt_sql_1236_good_" + suffix
	badLookup := "fvt_sql_1236_bad_" + suffix
	malformedLookup := "fvt_sql_1236_malformed_" + suffix
	goodURL := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)
	badURL := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", badPort)

	db, err := setupEmbeddedMysqlServer("127.0.0.1", port)
	s.Require().NoError(err)
	defer db.Close()
	// Blackhole: accept TCP and hold it without any MySQL handshake so a
	// dial blocks until timeout instead of failing fast with refused.
	blackhole, err := newBlackholeListener(badPort)
	s.Require().NoError(err)
	defer blackhole.Close()

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
	malformedConf := map[string]any{"dburl": "not-a-url"}
	resp, err := client.CreateConf("sources/sql/confKeys/"+goodConfKey, goodConf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	_, _ = GetResponseText(resp)
	resp, err = client.CreateConf("sources/sql/confKeys/"+badConfKey, badConf)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	_, _ = GetResponseText(resp)
	resp, err = client.CreateConf("sources/sql/confKeys/"+malformedConfKey, malformedConf)
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

	// A malformed URL is a static error: CREATE TABLE fails.
	status, body, err = createLookup(malformedLookup, malformedConfKey)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusBadRequest, status, string(body))

	// The unreachable (blackhole) database does NOT fail table creation:
	// the connection attaches lazily and returns immediately.
	started := time.Now()
	status, body, err = createLookup(badLookup, badConfKey)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, status, string(body))
	s.Require().Less(time.Since(started), 5*time.Second, "lookup creation must not wait for the database")

	// The lookup listing API stays available and shows both tables.
	listCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	listStarted := time.Now()
	status, body, err = doFVTRequest(listCtx, http.MethodGet, "tables?kind=lookup", "")
	cancel()
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, status, string(body))
	s.Require().Less(time.Since(listStarted), 15*time.Second)
	s.Require().Contains(string(body), goodLookup)
	s.Require().Contains(string(body), badLookup)

	for _, name := range []string{goodLookup, badLookup} {
		deleteFVTResource(s.T(), "tables/"+name)
	}
	deleteFVTResource(s.T(), "connections/"+goodConnectionID)
	deleteFVTResource(s.T(), "connections/"+badConnectionID)
}

// TestIssue1238LookupRecoversWhileRunning verifies the lookup recovery
// semantics: the table is created while the database is down, the first
// lookup reports once (op exception metric), the following lookups wait for
// recovery, and the rule resumes producing output once the database returns
// without any rule restart.
func (s *SQLConnectionRegressionTestSuite) TestIssue1238LookupRecoversWhileRunning() {
	port := freeTCPPort(s.T())
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	connectionID := "fvt-sql-1238-" + suffix
	confKey := "fvt-sql-1238-conf-" + suffix
	simConfKey := "fvt-sql-1238-sim-conf-" + suffix
	tableName := "fvt_sql_1238_tab_" + suffix
	streamName := "fvt_sql_1238_stream_" + suffix
	ruleID := "fvt_sql_1238_rule_" + suffix
	dbURL := fmt.Sprintf("mysql://root:@127.0.0.1:%d/test", port)

	// The database is not started yet; the named connection keeps retrying
	// in the background.
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

	resp, err = client.CreateConf("sources/sql/confKeys/"+confKey, map[string]any{
		"connectionSelector": connectionID,
	})
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	_, _ = GetResponseText(resp)
	resp, err = client.CreateConf("sources/simulator/confKeys/"+simConfKey, map[string]any{
		"data":     []map[string]any{{"a": 1, "b": 2}},
		"interval": "200ms",
		"loop":     true,
	})
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)
	_, _ = GetResponseText(resp)

	// CREATE TABLE succeeds although the database is unreachable.
	statement := fmt.Sprintf(`{"sql":"CREATE TABLE %s() WITH (DATASOURCE=\"t\", CONF_KEY=\"%s\", TYPE=\"sql\", KIND=\"lookup\", KEY=\"a\")"}`, tableName, confKey)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	status, body, err := doFVTRequest(ctx, http.MethodPost, "tables", statement)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, status, string(body))
	s.T().Cleanup(func() {
		deleteFVTResource(s.T(), "tables/"+tableName)
	})

	resp, err = client.CreateStream(fmt.Sprintf(`{
		"sql": "create stream %s () WITH (TYPE=\"simulator\", CONF_KEY=\"%s\", FORMAT=\"json\")"
	}`, streamName, simConfKey))
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	_, _ = GetResponseText(resp)
	s.T().Cleanup(func() {
		deleteFVTResource(s.T(), "streams/"+streamName)
	})
	resp, err = client.CreateRule(fmt.Sprintf(`{
		"id": %q,
		"sql": "SELECT %s.a AS a, %s.b AS b FROM %s INNER JOIN %s ON %s.a = %s.a",
		"actions": [{"log": {}}]
	}`, ruleID, streamName, tableName, streamName, tableName, tableName, streamName))
	s.Require().NoError(err)
	s.Require().Equal(http.StatusCreated, resp.StatusCode)
	_, _ = GetResponseText(resp)
	s.T().Cleanup(func() {
		deleteFVTResource(s.T(), "rules/"+ruleID)
	})

	waitForFVTNamed(s.T(), sqlFVTTimeout, "initial rule "+ruleID, func() bool {
		return getRuleStatus(s.T(), ruleID) == "running"
	})
	// The first lookup reports the unavailable connection exactly once:
	// the op exception metric must observe it.
	waitForFVTNamed(s.T(), sqlFVTTimeout, "first lookup exception", func() bool {
		return opExceptionsTotal(s.T(), ruleID) > 0
	})

	// Start the database; the rule recovers without restart.
	db, err := setupEmbeddedMysqlServer("127.0.0.1", port)
	s.Require().NoError(err)
	defer db.Close()

	waitForFVTNamed(s.T(), sqlFVTTimeout, "lookup recovery output", func() bool {
		return sinkRecordsOut(s.T(), ruleID) > 0
	})
}

// opExceptionsTotal sums per-op exception metrics of a rule. The lookup
// failure surfaces in the owning op (the lookup join node).
func opExceptionsTotal(t *testing.T, ruleID string) float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status, body, err := doFVTRequest(ctx, http.MethodGet, "rules/"+ruleID+"/status", "")
	if err != nil || status != http.StatusOK {
		return 0
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		return 0
	}
	var total float64
	for k, v := range result {
		if len(k) > len("op_") && strings.HasPrefix(k, "op_") && strings.HasSuffix(k, "_exceptions_total") {
			if f, ok := v.(float64); ok {
				total += f
			}
		}
	}
	return total
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

// sinkRecordsOut sums per-rule sink records_out metrics. Unlike the shared
// stream.log, metrics are per-rule and do not suffer from file rotation or
// cross-test pollution, so they are a more reliable readiness signal.
func sinkRecordsOut(t *testing.T, ruleID string) float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status, body, err := doFVTRequest(ctx, http.MethodGet, "rules/"+ruleID+"/status", "")
	if err != nil || status != http.StatusOK {
		return 0
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		return 0
	}
	var total float64
	for k, v := range result {
		if len(k) > len("sink_") && strings.HasPrefix(k, "sink_") && strings.HasSuffix(k, "_records_out_total") {
			if f, ok := v.(float64); ok {
				total += f
			}
		}
	}
	return total
}

func streamLogFileSize() int64 {
	fi, err := os.Stat(path.Join(PWD, "log", "stream.log"))
	if err != nil {
		return -1
	}
	return fi.Size()
}

// waitForSQLSourceOutput polls condition until timeout, logging per-rule
// diagnostics every 5s and a full dump on failure so CI logs show whether
// rules were running, the connection was up, metrics advanced, or the output
// files were missing.
func waitForSQLSourceOutput(t *testing.T, timeout time.Duration, name, connectionID string, ruleIDs []string, outFiles map[string]string, condition func() bool) {
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		if time.Since(lastLog) >= 5*time.Second {
			lastLog = time.Now()
			dumpSQLDiagnostics(t, connectionID, ruleIDs, outFiles)
		}
		time.Sleep(ConstantInterval)
	}
	dumpSQLDiagnostics(t, connectionID, ruleIDs, outFiles)
	t.Fatalf("%s condition was not satisfied before timeout", name)
}

func dumpSQLDiagnostics(t *testing.T, connectionID string, ruleIDs []string, outFiles map[string]string) {
	connStatus, ok := getConnectionStatus(t, connectionID)
	t.Logf("diag connection %s status=%q ok=%v", connectionID, connStatus, ok)
	for _, id := range ruleIDs {
		t.Logf("diag rule %s status=%q sinkOut=%v logAny=%d logB2=%d",
			id, getRuleStatus(t, id), sinkRecordsOut(t, id),
			streamLogCount(id, "sink result"), streamLogCount(id, "sink result", `\"b\":2`))
		if f, ok := outFiles[id]; ok {
			t.Logf("diag file rule %s path=%s size=%d", id, f, fileSize(f))
		}
	}
	t.Logf("diag stream.log size=%d", streamLogFileSize())
}

func fileSize(file string) int64 {
	fi, err := os.Stat(file)
	if err != nil {
		return -1
	}
	return fi.Size()
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

// blackholeListener accepts TCP connections and holds them open without
// sending anything, simulating a firewall DROP after accept. Dial blocks
// until timeout instead of failing fast with refused.
type blackholeListener struct {
	listener net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	closed   bool
}

func newBlackholeListener(port int) (*blackholeListener, error) {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	b := &blackholeListener{listener: l}
	go b.accept()
	return b, nil
}

func (b *blackholeListener) accept() {
	for {
		c, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			_ = c.Close()
			return
		}
		b.conns = append(b.conns, c)
		b.mu.Unlock()
	}
}

func (b *blackholeListener) Close() error {
	b.mu.Lock()
	b.closed = true
	conns := b.conns
	b.conns = nil
	b.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return b.listener.Close()
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

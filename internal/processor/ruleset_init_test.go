// Copyright 2025 EMQ Technologies Co., Ltd.
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

package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/lookup"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

type flakyDefinitionKV struct {
	kv.KeyValue
	key      string
	failures int
	attempts int
	getErr   error
	onSet    func()
}

func (f *flakyDefinitionKV) Set(key string, value interface{}) error {
	if key == f.key {
		f.attempts++
		if f.onSet != nil {
			f.onSet()
		}
		if f.attempts <= f.failures {
			return errors.New("temporary storage error")
		}
	}
	return f.KeyValue.Set(key, value)
}

func (f *flakyDefinitionKV) Get(key string, value interface{}) (bool, error) {
	if key == f.key && f.getErr != nil {
		return false, f.getErr
	}
	return f.KeyValue.Get(key, value)
}

func newDefinitionTestRuleset(t *testing.T) (*RulesetProcessor, *StreamProcessor, *RuleProcessor) {
	t.Helper()
	sp := NewStreamProcessor()
	rp := NewRuleProcessor()
	t.Cleanup(func() { _ = sp.db.Clean(); _ = rp.db.Clean() })
	return NewRulesetProcessor(rp, sp), sp, rp
}

func TestInitDefinitionWriteRetries(t *testing.T) {
	for _, tc := range []struct {
		kind, content, key string
		isRule             bool
		index              int
	}{
		{"stream", `{"streams":{"init_stream":"CREATE STREAM init_stream () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}}`, "init_stream", false, 0},
		{"table", `{"tables":{"init_table":"CREATE TABLE init_table () WITH (DATASOURCE=\"demo\", TYPE=\"memory\")"}}`, "init_table", false, 1},
		{"rule", `{"rules":{"init_rule":"{\"id\":\"init_rule\",\"sql\":\"SELECT * FROM init_stream\",\"actions\":[{\"log\":{}}]}"}}`, "init_rule", true, 2},
	} {
		for _, failures := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s/%d-failures", tc.kind, failures), func(t *testing.T) {
				rs, sp, rp := newDefinitionTestRuleset(t)
				flaky := &flakyDefinitionKV{key: tc.key, failures: failures}
				if tc.isRule {
					flaky.KeyValue = rp.db
					rp.db = flaky
				} else {
					flaky.KeyValue = sp.db
					sp.db = flaky
				}
				counts, failed, err := rs.ImportForInit([]byte(tc.content))
				require.NoError(t, err)
				require.Equal(t, 3, flaky.attempts)
				require.Equal(t, failures == 3, failed)
				if failures == 2 {
					require.Equal(t, 1, counts[tc.index])
				} else {
					require.Equal(t, []int{0, 0, 0}, counts)
				}
			})
		}
	}
}

func TestInitLookupWriteRetryDoesNotReconnect(t *testing.T) {
	rs, sp, _ := newDefinitionTestRuleset(t)
	const name = "init_lookup"
	t.Cleanup(func() { _ = lookup.DropInstance(name) })
	var first api.Source
	flaky := &flakyDefinitionKV{KeyValue: sp.db, key: name, failures: 2}
	flaky.onSet = func() {
		source, err := lookup.Attach(name)
		require.NoError(t, err)
		require.NoError(t, lookup.Detach(name))
		if first == nil {
			first = source
		} else {
			require.Same(t, first, source)
		}
	}
	sp.db = flaky
	counts, failed, err := rs.ImportForInit([]byte(`{"tables":{"init_lookup":"CREATE TABLE init_lookup () WITH (DATASOURCE=\"users\", TYPE=\"memory\", FORMAT=\"JSON\", KEY=\"id\", KIND=\"lookup\")"}}`))
	require.NoError(t, err)
	require.False(t, failed)
	require.Equal(t, []int{0, 1, 0}, counts)
	require.Equal(t, 3, flaky.attempts)
}

func TestInitVersionSkipAndSharedOrder(t *testing.T) {
	rs, sp, rp := newDefinitionTestRuleset(t)
	newer := []byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", VERSION=\"2\", SHARED=true)"},"rules":{"rule":"{\"id\":\"rule\",\"version\":\"2\",\"sql\":\"SELECT * FROM source\",\"actions\":[{\"log\":{}}]}"}}`)
	_, failed, err := rs.ImportForInit(newer)
	require.NoError(t, err)
	require.False(t, failed)
	older := []byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", VERSION=\"1\", SHARED=false)"},"rules":{"rule":"{\"id\":\"rule\",\"version\":\"1\",\"sql\":\"SELECT * FROM source\",\"actions\":[{\"log\":{}}]}"}}`)
	counts, failed, err := rs.ImportForInit(older)
	require.NoError(t, err)
	require.False(t, failed)
	require.Equal(t, []int{0, 0, 0}, counts)
	rules, apiCounts, err := rs.Import(older)
	require.NoError(t, err)
	require.Empty(t, rules)
	require.Equal(t, []int{0, 0, 0}, apiCounts)
	_, err = sp.ExecReplaceStream("source", `CREATE STREAM source () WITH (DATASOURCE="demo", FORMAT="JSON", VERSION="1", SHARED=false)`, ast.TypeStream)
	require.ErrorContains(t, err, "already exists with version")
	_, err = rp.ExecCreateWithValidation("rule", `{"id":"rule","version":"1","sql":"SELECT * FROM source","actions":[{"log":{}}]}`)
	require.ErrorContains(t, err, "already exists with version")
}

func TestInitCorruptDefinitionsAreReplaced(t *testing.T) {
	for _, tc := range []struct {
		name, kind, corrupt, definition string
	}{
		{"bad_stream", "stream", "not JSON", `CREATE STREAM bad_stream () WITH (DATASOURCE="demo", FORMAT="JSON")`},
		{"bad_stream_sql", "stream", `{"streamType":0,"statement":"not SQL"}`, `CREATE STREAM bad_stream_sql () WITH (DATASOURCE="demo", FORMAT="JSON")`},
		{"bad_table", "table", "not JSON", `CREATE TABLE bad_table () WITH (DATASOURCE="demo", TYPE="memory")`},
		{"bad_table_sql", "table", `{"streamType":1,"statement":"not SQL"}`, `CREATE TABLE bad_table_sql () WITH (DATASOURCE="demo", TYPE="memory")`},
		{"bad_rule", "rule", "not JSON", `{"id":"bad_rule","sql":"SELECT * FROM demo","actions":[{"log":{}}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs, sp, rp := newDefinitionTestRuleset(t)
			var content Ruleset
			var db kv.KeyValue
			switch tc.kind {
			case "stream":
				content.Streams, db = map[string]string{tc.name: tc.definition}, sp.db
			case "table":
				content.Tables, db = map[string]string{tc.name: tc.definition}, sp.db
			case "rule":
				content.Rules, db = map[string]string{tc.name: tc.definition}, rp.db
			}
			require.NoError(t, db.Set(tc.name, tc.corrupt))
			payload, err := json.Marshal(content)
			require.NoError(t, err)
			_, failed, err := rs.ImportForInit(payload)
			require.NoError(t, err)
			require.False(t, failed)
			var stored string
			exists, err := db.Get(tc.name, &stored)
			require.NoError(t, err)
			require.True(t, exists)
			if tc.kind == "rule" {
				require.Equal(t, tc.definition, stored)
			} else {
				var info struct {
					Statement string `json:"statement"`
				}
				require.NoError(t, json.Unmarshal([]byte(stored), &info))
				require.Equal(t, tc.definition, info.Statement)
			}
		})
	}
}

func TestInitReadErrorDoesNotWrite(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		isRule        bool
	}{
		{"source", `{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}}`, false},
		{"rule", `{"rules":{"rule":"{\"id\":\"rule\",\"sql\":\"SELECT * FROM source\",\"actions\":[{\"log\":{}}]}"}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs, sp, rp := newDefinitionTestRuleset(t)
			flaky := &flakyDefinitionKV{key: tc.name, getErr: errors.New("temporary read error")}
			if tc.isRule {
				flaky.KeyValue, rp.db = rp.db, flaky
			} else {
				flaky.KeyValue, sp.db = sp.db, flaky
			}
			_, failed, err := rs.ImportForInit([]byte(tc.content))
			require.NoError(t, err)
			require.True(t, failed)
			require.Zero(t, flaky.attempts)
		})
	}
}

func TestInitInvalidJSONFails(t *testing.T) {
	rs, _, _ := newDefinitionTestRuleset(t)
	_, _, err := rs.ImportForInit([]byte("not JSON"))
	require.ErrorContains(t, err, "invalid import file")
}

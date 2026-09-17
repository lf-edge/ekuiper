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
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/topo/lookup"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

type flakyInitKV struct {
	kv.KeyValue
	key      string
	failures int
	attempts int
	getErr   error
	onSet    func()
}

func (f *flakyInitKV) Set(key string, value interface{}) error {
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

func TestImportForInitLookupPersistenceDoesNotReconnect(t *testing.T) {
	rs, sp, _ := newInitTestRuleset(t)
	const name = "init_lookup"
	t.Cleanup(func() { _ = lookup.DropInstance(name) })
	var first api.Source
	flaky := &flakyInitKV{KeyValue: sp.db, key: name, failures: 2}
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
	result, err := rs.ImportForInit([]byte(`{"tables":{"init_lookup":"CREATE TABLE init_lookup () WITH (DATASOURCE=\"users\", TYPE=\"memory\", FORMAT=\"JSON\", KEY=\"id\", KIND=\"lookup\")"}}`))
	require.NoError(t, err)
	require.Equal(t, InitApplied, result.Objects[0].Outcome)
	require.Equal(t, 3, flaky.attempts)
}

func (f *flakyInitKV) Get(key string, value interface{}) (bool, error) {
	if key == f.key && f.getErr != nil {
		return false, f.getErr
	}
	return f.KeyValue.Get(key, value)
}

func newInitTestRuleset(t *testing.T) (*RulesetProcessor, *StreamProcessor, *RuleProcessor) {
	t.Helper()
	sp := NewStreamProcessor()
	rp := NewRuleProcessor()
	t.Cleanup(func() { _ = sp.db.Clean(); _ = rp.db.Clean() })
	return NewRulesetProcessor(rp, sp), sp, rp
}

func TestImportForInitRetriesPersistence(t *testing.T) {
	for _, tc := range []struct {
		kind, content, key string
		isRule             bool
	}{
		{"stream", `{"streams":{"init_stream":"CREATE STREAM init_stream () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}}`, "init_stream", false},
		{"table", `{"tables":{"init_table":"CREATE TABLE init_table () WITH (DATASOURCE=\"demo\", TYPE=\"memory\")"}}`, "init_table", false},
		{"rule", `{"rules":{"init_rule":"{\"id\":\"init_rule\",\"sql\":\"SELECT * FROM init_stream\",\"actions\":[{\"log\":{}}]}"}}`, "init_rule", true},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			rs, sp, rp := newInitTestRuleset(t)
			flaky := &flakyInitKV{key: tc.key, failures: 2}
			if tc.isRule {
				flaky.KeyValue = rp.db
				rp.db = flaky
			} else {
				flaky.KeyValue = sp.db
				sp.db = flaky
			}
			result, err := rs.ImportForInit([]byte(tc.content))
			require.NoError(t, err)
			require.Equal(t, 3, flaky.attempts)
			require.False(t, result.HasRetryableFailure())
			require.Equal(t, InitApplied, result.Objects[0].Outcome)
			require.Equal(t, 3, result.Objects[0].Attempts)
		})
	}
}

func TestImportForInitRetryExhaustedAndTerminal(t *testing.T) {
	rs, sp, _ := newInitTestRuleset(t)
	flaky := &flakyInitKV{KeyValue: sp.db, key: "failed_stream", failures: 3}
	sp.db = flaky
	content := []byte(`{"streams":{"failed_stream":"CREATE STREAM failed_stream () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")","bad_stream":"not SQL"},"rules":{"bad_rule":"not JSON"}}`)
	result, err := rs.ImportForInit(content)
	require.NoError(t, err)
	require.Equal(t, 3, flaky.attempts)
	require.True(t, result.HasRetryableFailure())
	require.Len(t, result.Objects, 3)
	for _, object := range result.Objects {
		if object.Name == "failed_stream" {
			require.Equal(t, InitRetryableError, object.Outcome)
			require.Equal(t, 3, object.Attempts)
		} else {
			require.Equal(t, InitTerminalError, object.Outcome)
		}
	}
	_, err = rs.ImportForInit([]byte("not JSON"))
	require.ErrorContains(t, err, "invalid import file")
	var formatErr *InitJSONError
	require.ErrorAs(t, err, &formatErr)
}

func TestImportForInitRetryExhaustedForRule(t *testing.T) {
	rs, _, rp := newInitTestRuleset(t)
	flaky := &flakyInitKV{KeyValue: rp.db, key: "failed_rule", failures: 3}
	rp.db = flaky
	result, err := rs.ImportForInit([]byte(`{"rules":{"failed_rule":"{\"id\":\"failed_rule\",\"sql\":\"SELECT * FROM demo\",\"actions\":[{\"log\":{}}]}"}}`))
	require.NoError(t, err)
	require.Equal(t, 3, flaky.attempts)
	require.True(t, result.HasRetryableFailure())
	require.Equal(t, InitRetryableError, result.Objects[0].Outcome)
}

func TestImportForInitSkipsOldVersions(t *testing.T) {
	rs, sp, rp := newInitTestRuleset(t)
	newer := []byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", VERSION=\"2\")"},"rules":{"rule":"{\"id\":\"rule\",\"version\":\"2\",\"sql\":\"SELECT * FROM source\",\"actions\":[{\"log\":{}}]}"}}`)
	_, err := rs.ImportForInit(newer)
	require.NoError(t, err)
	old := []byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", VERSION=\"1\")"},"rules":{"rule":"{\"id\":\"rule\",\"version\":\"1\",\"sql\":\"SELECT * FROM source\",\"actions\":[{\"log\":{}}]}"}}`)
	result, err := rs.ImportForInit(old)
	require.NoError(t, err)
	require.False(t, result.HasRetryableFailure())
	require.Equal(t, InitCounts{}, result.Counts)
	require.Len(t, result.Objects, 2)
	for _, object := range result.Objects {
		require.Equal(t, InitSkipped, object.Outcome)
		require.NoError(t, object.Err)
		require.Equal(t, 1, object.Attempts)
	}
	equal, err := rs.ImportForInit(newer)
	require.NoError(t, err)
	for _, object := range equal.Objects {
		require.Equal(t, InitSkipped, object.Outcome)
	}
	stream, err := sp.GetStream("source", ast.TypeStream)
	require.NoError(t, err)
	require.Contains(t, stream, `VERSION="2"`)
	rule, err := rp.GetRuleById("rule")
	require.NoError(t, err)
	require.Equal(t, "2", rule.Version)
}

func TestImportForInitLookupProvisionFailureIsNotImmediatelyRetried(t *testing.T) {
	rs, _, _ := newInitTestRuleset(t)
	result, err := rs.ImportForInit([]byte(`{"tables":{"bad_lookup":"CREATE TABLE bad_lookup () WITH (DATASOURCE=\"users\", TYPE=\"missing_source\", FORMAT=\"JSON\", KEY=\"id\", KIND=\"lookup\")"}}`))
	require.NoError(t, err)
	require.True(t, result.HasRetryableFailure())
	require.Equal(t, InitRetryableError, result.Objects[0].Outcome)
	require.Equal(t, 1, result.Objects[0].Attempts)
}

func TestImportForInitSharedConflictIsTerminal(t *testing.T) {
	rs, _, _ := newInitTestRuleset(t)
	_, err := rs.ImportForInit([]byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", SHARED=false)"}}`))
	require.NoError(t, err)
	result, err := rs.ImportForInit([]byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", SHARED=true)"}}`))
	require.NoError(t, err)
	require.False(t, result.HasRetryableFailure())
	require.Equal(t, InitTerminalError, result.Objects[0].Outcome)
	require.ErrorContains(t, result.Objects[0].Err, "SHARED")
	require.Equal(t, 1, result.Objects[0].Attempts)
}

func TestImportForInitUnknownReadErrorDoesNotRetryApply(t *testing.T) {
	rs, sp, _ := newInitTestRuleset(t)
	flaky := &flakyInitKV{KeyValue: sp.db, key: "source", getErr: errors.New("temporary read error")}
	sp.db = flaky
	result, err := rs.ImportForInit([]byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}}`))
	require.NoError(t, err)
	require.True(t, result.HasRetryableFailure())
	require.Equal(t, InitRetryableError, result.Objects[0].Outcome)
	require.Equal(t, 1, result.Objects[0].Attempts)
	require.Zero(t, flaky.attempts)

	rs, _, rp := newInitTestRuleset(t)
	flakyRule := &flakyInitKV{KeyValue: rp.db, key: "rule", getErr: errors.New("temporary rule read error")}
	rp.db = flakyRule
	ruleResult, err := rs.ImportForInit([]byte(`{"rules":{"rule":"{\"id\":\"rule\",\"sql\":\"SELECT * FROM source\",\"actions\":[{\"log\":{}}]}"}}`))
	require.NoError(t, err)
	require.Equal(t, InitRetryableError, ruleResult.Objects[0].Outcome)
	require.Equal(t, 1, ruleResult.Objects[0].Attempts)
	require.Zero(t, flakyRule.attempts)
}

func TestImportForInitReplacesCorruptStoredDefinition(t *testing.T) {
	for _, tc := range []struct {
		name, kind, corrupt, definition string
	}{
		{"bad_stream_json", "stream", "not JSON", `CREATE STREAM bad_stream_json () WITH (DATASOURCE="demo", FORMAT="JSON", VERSION="2")`},
		{"bad_stream_sql", "stream", `{"streamType":0,"statement":"not SQL"}`, `CREATE STREAM bad_stream_sql () WITH (DATASOURCE="demo", FORMAT="JSON", VERSION="2")`},
		{"bad_table_json", "table", "not JSON", `CREATE TABLE bad_table_json () WITH (DATASOURCE="demo", TYPE="memory", VERSION="2")`},
		{"bad_table_sql", "table", `{"streamType":1,"statement":"not SQL"}`, `CREATE TABLE bad_table_sql () WITH (DATASOURCE="demo", TYPE="memory", VERSION="2")`},
		{"bad_rule_json", "rule", "not JSON", `{"id":"bad_rule_json","version":"2","sql":"SELECT * FROM demo","actions":[{"log":{}}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs, sp, rp := newInitTestRuleset(t)
			var content Ruleset
			var db kv.KeyValue
			switch tc.kind {
			case "stream":
				content.Streams = map[string]string{tc.name: tc.definition}
				db = sp.db
			case "table":
				content.Tables = map[string]string{tc.name: tc.definition}
				db = sp.db
			case "rule":
				content.Rules = map[string]string{tc.name: tc.definition}
				db = rp.db
			}
			require.NoError(t, db.Set(tc.name, tc.corrupt))
			payload, err := json.Marshal(content)
			require.NoError(t, err)
			result, err := rs.ImportForInit(payload)
			require.NoError(t, err)
			require.False(t, result.HasRetryableFailure())
			require.Equal(t, InitApplied, result.Objects[0].Outcome)
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

func TestImportForInitVersionPrecedesSharedConflict(t *testing.T) {
	rs, sp, _ := newInitTestRuleset(t)
	newer := []byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", VERSION=\"2\", SHARED=true)"}}`)
	_, err := rs.ImportForInit(newer)
	require.NoError(t, err)
	older := []byte(`{"streams":{"source":"CREATE STREAM source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", VERSION=\"1\", SHARED=false)"}}`)
	result, err := rs.ImportForInit(older)
	require.NoError(t, err)
	require.Equal(t, InitSkipped, result.Objects[0].Outcome)
	require.NoError(t, result.Objects[0].Err)
	stored, err := sp.GetStream("source", ast.TypeStream)
	require.NoError(t, err)
	require.Contains(t, stored, `VERSION="2"`)
}

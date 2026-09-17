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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

type flakyInitKV struct {
	kv.KeyValue
	key      string
	failures int
	attempts int
}

func (f *flakyInitKV) Set(key string, value interface{}) error {
	if key == f.key {
		f.attempts++
		if f.attempts <= f.failures {
			return errors.New("temporary storage error")
		}
	}
	return f.KeyValue.Set(key, value)
}

func TestImportForInitRetriesEachObjectType(t *testing.T) {
	for _, tc := range []struct {
		kind    string
		content string
		key     string
		index   int
		isRule  bool
	}{
		{"stream", `{"streams":{"init_stream":"CREATE STREAM init_stream () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}}`, "init_stream", 0, false},
		{"table", `{"tables":{"init_table":"CREATE TABLE init_table () WITH (DATASOURCE=\"demo\", TYPE=\"memory\")"}}`, "init_table", 1, false},
		{"rule", `{"rules":{"init_rule":"{\"id\":\"init_rule\",\"sql\":\"SELECT * FROM init_stream\",\"actions\":[{\"log\":{}}]}"}}`, "init_rule", 2, true},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			sp := NewStreamProcessor()
			rp := NewRuleProcessor()
			t.Cleanup(func() { _ = sp.db.Clean(); _ = rp.db.Clean() })
			flaky := &flakyInitKV{KeyValue: sp.db, key: tc.key, failures: 2}
			if tc.isRule {
				flaky.KeyValue = rp.db
				rp.db = flaky
			} else {
				sp.db = flaky
			}
			result, err := NewRulesetProcessor(rp, sp).ImportForInit([]byte(tc.content))
			require.NoError(t, err)
			require.Equal(t, 3, flaky.attempts)
			require.Empty(t, result.Failures)
			require.Equal(t, 1, result.Counts[tc.index])
		})
	}
}

func TestImportForInitRetryExhaustedAndPermanent(t *testing.T) {
	sp := NewStreamProcessor()
	rp := NewRuleProcessor()
	t.Cleanup(func() { _ = sp.db.Clean(); _ = rp.db.Clean() })
	flaky := &flakyInitKV{KeyValue: sp.db, key: "failed_stream", failures: 3}
	sp.db = flaky
	content := []byte(`{"streams":{"failed_stream":"CREATE STREAM failed_stream () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")","bad_stream":"not SQL"},"rules":{"bad_rule":"not JSON"}}`)
	result, err := NewRulesetProcessor(rp, sp).ImportForInit(content)
	require.NoError(t, err)
	require.Equal(t, 3, flaky.attempts)
	require.True(t, result.HasRetryableFailure())
	require.Len(t, result.Failures, 3)
	for _, failure := range result.Failures {
		if failure.Name == "failed_stream" {
			require.True(t, failure.Retryable)
			require.Equal(t, 3, failure.Attempts)
		} else {
			require.False(t, failure.Retryable)
		}
	}
	_, err = NewRulesetProcessor(rp, sp).ImportForInit([]byte("not JSON"))
	require.ErrorContains(t, err, "invalid import file")
}

func TestImportForInitRetryExhaustedForTableAndRule(t *testing.T) {
	for _, tc := range []struct {
		kind    string
		key     string
		content string
		isRule  bool
	}{
		{"table", "failed_table", `{"tables":{"failed_table":"CREATE TABLE failed_table () WITH (DATASOURCE=\"demo\", TYPE=\"memory\")"}}`, false},
		{"rule", "failed_rule", `{"rules":{"failed_rule":"{\"id\":\"failed_rule\",\"sql\":\"SELECT * FROM demo\",\"actions\":[{\"log\":{}}]}"}}`, true},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			sp := NewStreamProcessor()
			rp := NewRuleProcessor()
			t.Cleanup(func() { _ = sp.db.Clean(); _ = rp.db.Clean() })
			flaky := &flakyInitKV{key: tc.key, failures: 3}
			if tc.isRule {
				flaky.KeyValue = rp.db
				rp.db = flaky
			} else {
				flaky.KeyValue = sp.db
				sp.db = flaky
			}
			result, err := NewRulesetProcessor(rp, sp).ImportForInit([]byte(tc.content))
			require.NoError(t, err)
			require.Equal(t, 3, flaky.attempts)
			require.True(t, result.HasRetryableFailure())
			require.Len(t, result.Failures, 1)
			require.Equal(t, 3, result.Failures[0].Attempts)
		})
	}
}

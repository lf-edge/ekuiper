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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLeadUntilE2E exercises SQL planning, delayed analytic execution, projection,
// and EOF finalization through the REST ruletest API and its SSE sink.
func TestLeadUntilE2E(t *testing.T) {
	const streamName, ruleID = "leadUntilFVT", "lead_until_fvt"
	cleanup := func() {
		if resp, err := client.Delete("ruletest/" + ruleID); err == nil {
			resp.Body.Close()
		}
		if resp, err := client.DeleteStream(streamName); err == nil {
			resp.Body.Close()
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	stream, err := json.Marshal(map[string]string{"sql": fmt.Sprintf(`CREATE STREAM %s () WITH (TYPE="mqtt", DATASOURCE="%s", FORMAT="json")`, streamName, streamName)})
	require.NoError(t, err)
	resp, err := client.CreateStream(string(stream))
	require.NoError(t, err)
	body, err := GetResponseText(resp)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)

	definition, err := json.Marshal(map[string]any{
		"id":  ruleID,
		"sql": fmt.Sprintf(`SELECT ts, lead(v, 1, -1) OVER (WHEN matched UNTIL ts - current_row(ts) > 5) AS next_v, next_v - v AS delta FROM %s`, streamName),
		"mockSource": map[string]any{streamName: map[string]any{
			"loop": false, "interval": "100ms",
			"data": []map[string]any{
				{"ts": 0, "matched": false, "v": 100},
				{"ts": 4, "matched": false, "v": 200},
				{"ts": 6, "matched": true, "v": 300},
				{"ts": 12, "matched": false, "v": 400},
			},
		}},
		"sinkProps": map[string]any{"sendSingle": true},
	})
	require.NoError(t, err)
	resp, err = client.Post("ruletest", string(definition))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	result, err := GetResponseResultMap(resp)
	require.NoError(t, err)
	require.Equal(t, ruleID, result["id"])
	port, ok := result["port"].(float64)
	require.True(t, ok)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/test/%s", int(port), ruleID), nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "text/event-stream")
	sse, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer sse.Body.Close()
	require.Equal(t, http.StatusOK, sse.StatusCode)
	// Connect before starting the finite source so every output is observable.
	resp, err = client.Post("ruletest/"+ruleID+"/start", "any")
	require.NoError(t, err)
	body, err = GetResponseText(resp)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)

	expected := []map[string]any{
		// The probe at ts=6 expires ts=0 before it can match, but resolves ts=4.
		{"ts": float64(0), "next_v": float64(-1), "delta": float64(-101)},
		{"ts": float64(4), "next_v": float64(300), "delta": float64(100)},
		// ts=12 expires ts=6 without matching; EOF resolves the final row.
		{"ts": float64(6), "next_v": float64(-1), "delta": float64(-301)},
		{"ts": float64(12), "next_v": float64(-1), "delta": float64(-401)},
	}
	var got []map[string]any
	scanner := bufio.NewScanner(sse.Body)
	for scanner.Scan() {
		payload, ok := strings.CutPrefix(scanner.Text(), "data: ")
		if !ok {
			continue
		}
		var row map[string]any
		require.NoError(t, json.Unmarshal([]byte(payload), &row), payload)
		got = append(got, row)
		if len(got) == len(expected) {
			break
		}
	}
	require.NoError(t, scanner.Err(), "expected four delayed outputs, including the EOF tail")
	require.Equal(t, expected, got)
}

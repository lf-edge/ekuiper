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

package xsql

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseLargeSQLDoesNotIncludeInputInError(t *testing.T) {
	sql := "SELECT " + strings.Repeat("field,", 50_000) + " FROM"
	_, err := GetStatementFromSql(sql)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("bytes=%d, sha256=%x", len(sql), sha256.Sum256([]byte(sql))))
	require.NotContains(t, err.Error(), strings.Repeat("field,", 10))
	require.Less(t, len(err.Error()), 1024)
}

func TestGetRuleStreams(t *testing.T) {
	tests := []struct {
		sql     string
		streams []string
	}{
		{
			sql:     "SELECT device FROM roomA",
			streams: []string{"roomA"},
		},
		{
			sql:     "SELECT device FROM roomA UNION SELECT device FROM roomB",
			streams: []string{"roomA", "roomB"},
		},
		{
			sql:     "SELECT device FROM roomA UNION ALL SELECT device FROM roomB",
			streams: []string{"roomA", "roomB"},
		},
		{
			sql:     "SELECT device FROM roomA INTERSECT SELECT device FROM roomB",
			streams: []string{"roomA", "roomB"},
		},
		{
			sql:     "SELECT device FROM roomA WHERE temperature > 20 EXCEPT ALL SELECT device FROM roomB;",
			streams: []string{"roomA", "roomB"},
		},
	}
	for _, tt := range tests {
		stmt, err := GetStatementFromSql(tt.sql)
		require.NoError(t, err, tt.sql)
		require.Equal(t, tt.streams, GetRuleStreams(stmt), tt.sql)
		// GetStreams keeps returning the streams of the left statement only
		require.Equal(t, tt.streams[:1], GetStreams(stmt), tt.sql)
	}
	require.Nil(t, GetRuleStreams(nil))
}

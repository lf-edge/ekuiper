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

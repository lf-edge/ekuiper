// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package sql

import (
	"database/sql"
	"regexp"
)

type Database interface {
	Apply(f func(db *sql.DB) error) error
}

// isValidTableName checks if the given string is a valid database table name.
func isValidTableName(tableName string) bool {
	// Check if the table name is empty
	if tableName == "" {
		return false
	}

	// Regular expression to match valid table names
	// ^[a-zA-Z_][a-zA-Z0-9_]*$
	// ^[a-zA-Z_] ensures the name starts with a letter or underscore
	// [a-zA-Z0-9_]*$ ensures the rest of the name consists of letters, digits, or underscores
	validTableNamePattern := `^[a-zA-Z_][a-zA-Z0-9/_]*$`

	// Compile the regular expression
	re := regexp.MustCompile(validTableNamePattern)

	// Check if the table name matches the pattern
	return re.MatchString(tableName)
}

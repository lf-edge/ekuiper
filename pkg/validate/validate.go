// Copyright 2024 EMQ Technologies Co., Ltd.
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

package validate

import (
	"fmt"
	"regexp"
	"strings"
)

var idRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)

func ValidateID(id string) error {
	if id == "" {
		return fmt.Errorf("id cannot be empty")
	}
	if id != strings.TrimSpace(id) {
		return fmt.Errorf("id '%s' contains leading or trailing whitespace", id)
	}
	if !idRegex.MatchString(id) {
		return fmt.Errorf("id '%s' contains invalid characters: only alphanumeric, hyphens and underscores are allowed", id)
	}
	return nil
}

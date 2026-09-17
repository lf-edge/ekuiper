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
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type replaceDecision uint8

const (
	replaceApply replaceDecision = iota
	replaceSkip
	replaceSharedConflict
)

// Version precedence is checked before whether the new source could change
// SHARED, because a skipped source never attempts that change.
func decideSourceReplace(old, next *ast.StreamStmt) replaceDecision {
	if old == nil {
		return replaceApply
	}
	if !CanReplace(old.Options.VERSION, next.Options.VERSION) {
		return replaceSkip
	}
	if old.Options.SHARED != next.Options.SHARED {
		return replaceSharedConflict
	}
	return replaceApply
}

func decideRuleReplace(old, next *def.Rule) replaceDecision {
	if old != nil && !CanReplace(old.Version, next.Version) {
		return replaceSkip
	}
	return replaceApply
}

func applyOnce(write func() error) error { return write() }

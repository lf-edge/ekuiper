// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package ast

import (
	"regexp"
	"strings"
	"testing"
)

func compileByReplaceAll(likestr string) (*regexp.Regexp, error) {
	likestr = strings.ReplaceAll(strings.ReplaceAll(likestr, `\%`, `!@#`), `\_`, `!@$`)
	regstr := strings.ReplaceAll(strings.ReplaceAll(likestr, "%", ".*"), "_", ".")
	regstr = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(regstr, `!@$`, `\_`), `!@#`, `\%`), `\`, `\\`)
	return regexp.Compile("^" + regstr + "$")
}

func TestLikePatternCompile(t *testing.T) {
	l := LikePattern{}
	tests := []string{
		`%`, `_`, `\`, `\\`, `\%`, `\_`, `\\%`, `\\_`, `%\\%`, `%\\_`, `%\\%\\%`, `%\\%\\_`, `%\\%\\%`, `%\\%\\_`,
	}
	for i, test := range tests {
		expect, err := compileByReplaceAll(test)
		if err != nil {
			panic(err)
		}
		actual, err := l.Compile(test)
		if err != nil {
			panic(err)
		}
		if expect.String() != actual.String() {
			t.Errorf("%d. \nexpect: %s, actual: %s\n", i, expect, actual)
		}
	}
}

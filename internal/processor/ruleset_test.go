// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestIO(t *testing.T) {
	expected := `{"streams":{"demo":"CREATE STREAM demo () WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"},"tables":{},"rules":{"rule1":"{\"id\":\"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{\"log\": {}}]}","rule2":"{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}"}}`
	expectedCounts := []int{1, 0, 2}
	expectedStreams := []string{"demo"}
	expectedRules := []string{"rule1", "rule2"}
	sp := NewStreamProcessor()
	defer sp.db.Clean()
	rp := NewRuleProcessor()
	defer rp.db.Clean()
	rsp := NewRulesetProcessor(rp, sp)

	names, counts, err := rsp.Import([]byte(expected))
	if err != nil {
		t.Errorf("fail to import ruleset: %v", err)
		return
	}
	sort.Strings(names)
	if !reflect.DeepEqual(names, expectedRules) {
		t.Errorf("fail to return the imported rules, expect %v but got %v", expectedRules, names)
	}
	if !reflect.DeepEqual(counts, expectedCounts) {
		t.Errorf("fail to return the correct counts, expect %v, but got %v", expectedCounts, counts)
	}

	streams, err := sp.execShow(ast.TypeStream)
	if err != nil {
		t.Errorf("fail to get all streams: %v", err)
		return
	}
	if !reflect.DeepEqual(streams, expectedStreams) {
		t.Errorf("After import, expect streams %v, but got %v", expectedStreams, streams)
		return
	}

	rules, err := rp.GetAllRules()
	if err != nil {
		t.Errorf("fail to get all rules: %v", err)
		return
	}
	sort.Strings(rules)
	if !reflect.DeepEqual(rules, expectedRules) {
		t.Errorf("After import, expect rules %v, but got %v", expectedRules, rules)
		return
	}

	exp, exCounts, err := rsp.Export()
	if err != nil {
		t.Errorf("fail to export ruleset: %v", err)
		return
	}
	buf := new(strings.Builder)
	_, err = io.Copy(buf, exp)
	if err != nil {
		t.Errorf("fail to convert exported ruleset: %v", err)
		return
	}
	actual := buf.String()
	if actual != expected {
		t.Errorf("Expect\t\n %v but got\t\n %v", expected, actual)
	}
	if !reflect.DeepEqual(exCounts, expectedCounts) {
		t.Errorf("fail to return the correct counts, expect %v, but got %v", expectedCounts, exCounts)
	}
}

func TestImportError(t *testing.T) {
	contents := []string{
		"notjson",
		`{INvalid"streams":{"demo":"CREATE STREAM demo () WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"},"tables":{},"rules":{"rule1":"{\"id\":\"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{\"log\": {}}]}","rule2":"{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}"}}`,
	}
	sp := NewStreamProcessor()
	defer sp.db.Clean()
	rp := NewRuleProcessor()
	defer rp.db.Clean()
	rsp := NewRulesetProcessor(rp, sp)

	for i, content := range contents {
		_, _, err := rsp.Import([]byte(content))
		if err == nil {
			t.Errorf("%d fail, expect error but pass", i)
		} else {
			fmt.Println(err)
		}
	}
}

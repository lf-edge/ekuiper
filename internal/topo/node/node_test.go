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

package node

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
)

func TestOutputs(t *testing.T) {
	n := newDefaultNode("test", &def.RuleOption{})
	err := n.AddOutput(make(chan any), "rule.1_test")
	assert.NoError(t, err)
	err = n.AddOutput(make(chan any), "rule.2_test")
	assert.NoError(t, err)
	err = n.RemoveOutput("rule.1")
	assert.NoError(t, err)
	err = n.RemoveOutput("rule.4")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(n.outputs))
}

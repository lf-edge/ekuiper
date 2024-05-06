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

package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

func TestGetMetrics(t *testing.T) {
	ctx := mockContext.NewMockContext("rule1", "op1")
	sm := NewStatManager(ctx, "op")
	sm.ProcessTimeStart()
	sm.IncTotalRecordsIn()
	sm.SetBufferLength(20)
	a := sm.GetMetrics()
	e := []any{
		int64(1), int64(0), int64(0), int64(0), int64(20), "current time", int64(0), "", int64(0),
	}
	assert.Equal(t, e[:5], a[:5])
	assert.NotEqual(t, "", a[5])
	assert.Equal(t, e[6:], a[6:])
}

// Copyright 2023 EMQ Technologies Co., Ltd.
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

package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultSourceTupleResult(t *testing.T) {
	now := time.Now()

	st := NewDefaultSourceTupleWithTime(nil, nil, now)

	assert.Equal(t, &DefaultSourceTuple{
		Mess: nil,
		M:    nil,
		Time: now,
	}, st)
	assert.Nil(t, st.Message())
	assert.Nil(t, st.Meta())
	assert.Equal(t, now, st.Timestamp())

	st = NewDefaultSourceTuple(nil, nil)

	assert.Equal(t, &DefaultSourceTuple{
		Mess: nil,
		M:    nil,
		Time: st.Time,
	}, st)
	assert.Nil(t, st.Message())
	assert.Nil(t, st.Meta())
	assert.NotEqual(t, now, st.Timestamp())
}

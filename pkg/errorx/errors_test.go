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

package errorx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorResult(t *testing.T) {
	err := New("general error")

	assert.Equal(t, &Error{
		"general error",
		GENERAL_ERR,
	}, err)
	assert.Equal(t, "general error", err.Error())
	assert.Equal(t, GENERAL_ERR, err.Code())

	err = NewWithCode(NOT_FOUND, "not found")
	assert.Equal(t, &Error{
		"not found",
		NOT_FOUND,
	}, err)
	assert.Equal(t, "not found", err.Error())
	assert.Equal(t, NOT_FOUND, err.Code())
}

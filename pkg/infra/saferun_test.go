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

package infra

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDrainError(t *testing.T) {
	errChan := make(chan error, 1)
	err := errors.New("test error")

	go DrainError(nil, err, errChan)

	assert.Equal(t, err, <-errChan)
}

func TestSafeRun(t *testing.T) {
	tests := []struct {
		fn       func() error
		expected error
	}{
		{
			func() error {
				return nil
			},
			nil,
		},
		{
			func() error {
				return errors.New("test error")
			},
			errors.New("test error"),
		},
		{
			func() error {
				panic("panic error")
			},
			errors.New("panic error"),
		},
		{
			func() error {
				panic(2)
			},
			fmt.Errorf("%#v", 2),
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, SafeRun(tt.fn))
	}
}

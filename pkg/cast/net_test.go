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

package cast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJoinHostPortInt(t *testing.T) {
	tests := []struct {
		host string
		port int
		want string
	}{
		{
			"0.0.0.0",
			8080,
			"0.0.0.0:8080",
		},
		{
			"0.0.0.0",
			0,
			"0.0.0.0:0",
		},
		{
			"::1",
			8080,
			"[::1]:8080",
		},
		{
			"example.com",
			8080,
			"example.com:8080",
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, JoinHostPortInt(tt.host, tt.port))
	}
}

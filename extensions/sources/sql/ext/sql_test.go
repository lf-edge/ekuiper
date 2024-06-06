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

package sql

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestHandleReconnect(t *testing.T) {
	m := &sqlsource{
		conf: &sqlConConfig{
			Interval: 10,
		},
	}
	ch := make(chan api.SourceTuple, 1)

	failpoint.Enable("github.com/lf-edge/ekuiper/extensions/sources/sql/ext/handleReconnectErr", "return(1)")
	require.False(t, m.handleReconnect(ch))
	errTuple := <-ch
	require.NotNil(t, errTuple)

	failpoint.Enable("github.com/lf-edge/ekuiper/extensions/sources/sql/ext/handleReconnectErr", "return(2)")
	require.True(t, m.handleReconnect(ch))
	failpoint.Disable("github.com/lf-edge/ekuiper/extensions/sources/sql/ext/handleReconnectErr")
}

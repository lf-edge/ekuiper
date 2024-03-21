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

package service

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	e := &httpExecutor{
		restOpt: &restOption{
			RetryCount:            2,
			RetryInterval:         "1ms",
			retryIntervalDuration: time.Microsecond,
		},
	}
	require.NoError(t, failpoint.Enable("github.com/lf-edge/ekuiper/internal/service/httpExecutorRetry", "return(true)"))
	defer func() {
		failpoint.Disable("github.com/lf-edge/ekuiper/internal/service/httpExecutorRetry")
	}()
	_, err := e.InvokeFunction(nil, "", nil)
	require.NoError(t, err)
}

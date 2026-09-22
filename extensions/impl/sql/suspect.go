// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

// reportTransportFailure reports one failed business I/O against the
// pooled connection — unless the caller's own scope is already dead.
// A canceled rule context (stop, deadline, manual cancel) makes
// Query/Exec fail with ctx.Err(): that is caller death, not transport
// failure, and must never close the shared readiness gate and park
// unrelated rules on the same connection. Best-effort isolation: a
// caller dying between this check and the report still reports, and
// the Pool's server-owned verification falsifies it — never a
// correctness issue, at most one redundant verify.
func reportTransportFailure(ctx api.StreamContext, cw *connection.ConnWrapper) {
	if ctx.Err() == nil {
		cw.ReportSuspectedFailure()
	}
}

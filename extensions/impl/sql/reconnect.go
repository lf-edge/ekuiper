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
	"github.com/cenkalti/backoff/v4"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	client2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

// retryReconnect drives the runtime reconnect loop shared by the SQL source,
// sink and lookup. It sets no total retry count or time limit: retries
// continue until a single attempt succeeds or the caller (rule) context is
// canceled, so a database outage only delays the next business operation
// instead of failing the rule.
//
// SQLConnection.Reconnect stays a single bounded attempt; the retry policy
// deliberately lives here so moving retry ownership into the connection
// pool later does not touch SQLConnection again.
func retryReconnect(ctx api.StreamContext, conn *client2.SQLConnection) error {
	// Runtime retry explicitly chooses an unlimited elapsed time.
	// Connection-pool retry policy remains independent.
	bo := connection.NewExponentialBackOffWithMaxElapsedTime(0)
	return backoff.Retry(func() error {
		// Fail fast when the caller is gone: the next business call
		// re-enters recovery.
		if err := ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}
		return conn.Reconnect(ctx)
	}, backoff.WithContext(bo, ctx))
}

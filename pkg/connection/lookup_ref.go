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

package connection

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

// lookupRefIDKey carries the framework-owned lookup resource identity
// ("lookup:" + table name). It lives in the connection package instead of
// the lookup framework package to avoid an import cycle:
// extensions/impl/sql -> internal/binder/io -> internal/topo/lookup.
type lookupRefIDKey struct{}

// WithLookupRefID injects the framework-owned lookup resource identity into
// ctx. Called by lookup.CreateInstance before LookupSource.Connect; the
// source only reads and stores the value, never derives it.
//
// The parameter is deliberately *topoContext.DefaultContext, the only
// context the lookup framework ever creates: there is no production need
// to inject into an arbitrary api.StreamContext, and wrapping one would
// silently drop its rule/op/instance identity.
func WithLookupRefID(ctx *topoContext.DefaultContext, refID string) *topoContext.DefaultContext {
	return topoContext.WithValue(ctx, lookupRefIDKey{}, refID)
}

// LookupRefID reads the framework-injected lookup resource identity.
// The second return value reports whether a non-empty identity was present.
func LookupRefID(ctx api.StreamContext) (string, bool) {
	if ctx == nil {
		return "", false
	}
	v := ctx.Value(lookupRefIDKey{})
	s, ok := v.(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

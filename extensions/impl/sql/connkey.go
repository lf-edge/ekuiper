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

import "fmt"

// sqlConnectionKey derives the Pool connectionKey for the new explicit
// fetch path (DESIGN §8, A1a).
//
//   - selector absent (or explicitly empty): anonymous canonical key, i.e.
//     the resolved DB URL (dburl wins over url, see SQLConf.resolveDBURL).
//     Phase 0 performs no further URL normalization.
//   - selector is a non-empty string: reference to an existing named
//     connection, fetch must not create (requireExisting).
//   - selector present with a non-string value (nil counts as absent,
//     mirroring legacy decoding where null means unset): static
//     configuration error. Silently falling back to anonymous would switch
//     the connection model behind the user's back.
func sqlConnectionKey(props map[string]any, resolvedDBURL string) (key string, requireExisting bool, err error) {
	if v, ok := props["connectionSelector"]; ok && v != nil {
		s, ok := v.(string)
		if !ok {
			return "", false, fmt.Errorf("connectionSelector must be a string, got %T", v)
		}
		if s != "" {
			return s, true, nil
		}
	}
	return resolvedDBURL, false, nil
}

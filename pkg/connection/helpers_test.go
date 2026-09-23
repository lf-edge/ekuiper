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

// Test-only read helper: reports the live reference count for key, or
// 0 when the key is absent or mid-transition. Production code never
// needs it — ownership flows through Lease Release.
func getConnectionRef(id string) int {
	m := globalConnectionManager.Load()
	m.RLock()
	defer m.RUnlock()
	meta, err := readyMeta(m, id)
	if err != nil || meta == nil {
		return 0
	}
	return meta.GetRefCount()
}

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

package random

import (
	"fmt"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// benchmarkDedupSteadyState measures Pull once the bounded dedup list is at
// capacity, so calls take the copy-on-write eviction path. The large seed
// keeps random collisions negligible.
func benchmarkDedupSteadyState(b *testing.B, limit int) {
	b.Helper()
	ctx := mockContext.NewMockContext("rule", "random")
	source := &randomSource{conf: &randomSourceConfig{
		Pattern:     map[string]interface{}{"value": 1},
		Seed:        1 << 30,
		Deduplicate: limit,
	}}
	for i := 0; i < limit; i++ {
		source.list = append(source.list, []byte(fmt.Sprintf(`{"value":%d}`, i)))
	}
	ingest := func(_ api.StreamContext, _ any, _ map[string]any, _ time.Time) {}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = i
		source.Pull(ctx, time.Now(), ingest, func(_ api.StreamContext, _ error) {})
	}
}

func BenchmarkDedupSteadyState1K(b *testing.B) {
	benchmarkDedupSteadyState(b, 1_000)
}

func BenchmarkDedupSteadyState10K(b *testing.B) {
	benchmarkDedupSteadyState(b, 10_000)
}

func BenchmarkDedupSteadyState100K(b *testing.B) {
	benchmarkDedupSteadyState(b, 100_000)
}

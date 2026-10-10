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
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func pullOnce(t *testing.T, source *randomSource, ctx api.StreamContext, ingested *int) {
	t.Helper()
	source.Pull(ctx, time.Now(),
		func(_ api.StreamContext, _ any, _ map[string]any, _ time.Time) {
			if ingested != nil {
				*ingested++
			}
		},
		func(_ api.StreamContext, _ error) {})
}

func TestDedupOffsetOwnsRestoredState(t *testing.T) {
	restored := [][]byte{[]byte("first"), []byte("second")}
	source := &randomSource{conf: &randomSourceConfig{Deduplicate: -1}}
	if err := source.Rewind(restored); err != nil {
		t.Fatal(err)
	}

	restored[0][0] = 'X'
	offset, err := source.GetOffset()
	if err != nil {
		t.Fatal(err)
	}
	got := offset.([][]byte)
	if !bytes.Equal(got[0], []byte("first")) {
		t.Fatalf("source shared restored checkpoint state: %#v", got)
	}
}

func TestDedupUsesImmutableRewindableOffsetInsteadOfLiveContextState(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "random")
	source := &randomSource{conf: &randomSourceConfig{
		Pattern:     map[string]interface{}{"value": 1},
		Seed:        1 << 30,
		Deduplicate: -1,
	}}
	var ingested int
	pullOnce(t, source, ctx, &ingested)
	requireIngested(t, ingested, 1)
	if state, err := ctx.GetState(dedupStateKey); err != nil {
		t.Fatal(err)
	} else if state != nil {
		t.Fatalf("random source published live dedup state to context: %#v", state)
	}

	offset, err := source.GetOffset()
	if err != nil {
		t.Fatal(err)
	}
	published := offset.([][]byte)
	if cap(published) != len(published) {
		t.Fatalf("published offset retains mutable append capacity: len %d cap %d", len(published), cap(published))
	}
	pullOnce(t, source, ctx, &ingested)
	before := cloneDedupList(published)
	if len(published) != len(before) {
		t.Fatalf("published offset changed after append: %#v", published)
	}
	for i := range published {
		if !bytes.Equal(published[i], before[i]) {
			t.Fatalf("published offset changed after append: %#v", published)
		}
	}
}

func requireIngested(t *testing.T, ingested, want int) {
	t.Helper()
	if ingested != want {
		t.Fatalf("ingested %d rows, want %d", ingested, want)
	}
}

func TestBoundedDedupUsesCopyOnWriteForPublishedOffset(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "random")
	source := &randomSource{
		conf: &randomSourceConfig{Deduplicate: 2},
		list: [][]byte{[]byte(`{"value":1}`), []byte(`{"value":2}`)},
	}
	offset, err := source.GetOffset()
	if err != nil {
		t.Fatal(err)
	}
	published := offset.([][]byte)

	dup, encoded := source.checkDup(ctx, map[string]interface{}{"value": int64(3)})
	if dup {
		t.Fatal("third value must not be a duplicate")
	}
	source.recordSeen(encoded)
	if !bytes.Equal(published[0], []byte(`{"value":1}`)) || !bytes.Equal(published[1], []byte(`{"value":2}`)) {
		t.Fatalf("published bounded offset changed during eviction: %#v", published)
	}
	if len(source.list) != 2 || !bytes.Equal(source.list[0], []byte(`{"value":2}`)) || !bytes.Equal(source.list[1], []byte(`{"value":3}`)) {
		t.Fatalf("unexpected current bounded dedup state: %#v", source.list)
	}
}

func TestPublishedDedupOffsetCanFreezeWhileSourceAdvances(t *testing.T) {
	for _, deduplicate := range []int{-1, 1_024} {
		t.Run(fmt.Sprintf("deduplicate=%d", deduplicate), func(t *testing.T) {
			initial := make([][]byte, 1_024, 2_048)
			for i := range initial {
				initial[i] = []byte("initial")
			}
			source := &randomSource{
				conf: &randomSourceConfig{
					Pattern:     map[string]interface{}{"value": 1},
					Seed:        1 << 30,
					Deduplicate: deduplicate,
				},
				list: initial,
			}
			offset, err := source.GetOffset()
			if err != nil {
				t.Fatal(err)
			}

			start := make(chan struct{})
			frozen := make(chan []byte, 1)
			encodeErr := make(chan error, 1)
			var ready sync.WaitGroup
			ready.Add(1)
			go func() {
				ready.Done()
				<-start
				var encoded []byte
				for range 20 {
					var encodeStateErr error
					encoded, encodeStateErr = checkpoint.EncodeState(map[string]interface{}{"offset": offset})
					if encodeStateErr != nil {
						encodeErr <- encodeStateErr
						return
					}
				}
				frozen <- encoded
			}()
			ready.Wait()
			close(start)
			ctx := mockContext.NewMockContext("rule", "random")
			for range 100 {
				pullOnce(t, source, ctx, nil)
			}

			select {
			case err := <-encodeErr:
				t.Fatal(err)
			case encoded := <-frozen:
				restored, err := checkpoint.DecodeState(encoded)
				if err != nil {
					t.Fatal(err)
				}
				published := restored["offset"].([][]byte)
				if len(published) != len(initial) {
					t.Fatalf("published offset length changed while freezing: got %d, want %d", len(published), len(initial))
				}
				if !bytes.Equal(published[0], []byte("initial")) || !bytes.Equal(published[len(published)-1], []byte("initial")) {
					t.Fatalf("published offset changed while freezing: len=%d first=%q last=%q", len(published), published[0], published[len(published)-1])
				}
			}
		})
	}
}

func TestDedupConnectClonesLegacyState(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "random")
	legacy := [][]byte{[]byte("first")}
	if err := ctx.PutState(dedupStateKey, legacy); err != nil {
		t.Fatal(err)
	}
	source := &randomSource{conf: &randomSourceConfig{Deduplicate: -1}}
	if err := source.Connect(ctx, func(string, string) {}); err != nil {
		t.Fatal(err)
	}

	legacy[0][0] = 'X'
	if !bytes.Equal(source.list[0], []byte("first")) {
		t.Fatalf("source shared legacy context state: %#v", source.list)
	}
}

// TestDedupOffsetExcludesUndeliveredRows proves the checkpoint boundary: a
// checkpoint racing a pull observes only rows already handed to the
// topology. The ingest callback reads the offset mid-delivery and must not
// see the row being delivered; after Pull returns it must be there.
func TestDedupOffsetExcludesUndeliveredRows(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "random")
	source := &randomSource{conf: &randomSourceConfig{
		Pattern:     map[string]interface{}{"value": 1},
		Seed:        1 << 30,
		Deduplicate: -1,
	}}
	var midIngest [][]byte
	source.Pull(ctx, time.Now(),
		func(_ api.StreamContext, _ any, _ map[string]any, _ time.Time) {
			offset, err := source.GetOffset()
			if err != nil {
				t.Errorf("mid-ingest offset read failed: %v", err)
				return
			}
			midIngest = append(midIngest, offset.([][]byte)...)
		},
		func(_ api.StreamContext, _ error) {})
	if len(midIngest) != 0 {
		t.Fatalf("checkpoint observed an undelivered row: %#v", midIngest)
	}
	offset, err := source.GetOffset()
	if err != nil {
		t.Fatal(err)
	}
	if got := offset.([][]byte); len(got) != 1 {
		t.Fatalf("delivered row missing from offset after pull: %#v", got)
	}
}

// TestDedupCheckpointConcurrentWithPull runs pulls against concurrent
// offset reads, rewinds and freezes, modeling PrepareCheckpoint racing
// ingestion on the coordinator goroutine. The race detector is the main
// assertion; the final state must stay well-formed.
func TestDedupCheckpointConcurrentWithPull(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "random")
	source := &randomSource{conf: &randomSourceConfig{
		Pattern:     map[string]interface{}{"value": 1},
		Seed:        1 << 30,
		Deduplicate: 64,
	}}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if _, err := source.GetOffset(); err != nil {
					t.Errorf("concurrent offset read failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 10 {
			offset, err := source.GetOffset()
			if err != nil {
				t.Errorf("rewind source read failed: %v", err)
				return
			}
			if err := source.Rewind(offset); err != nil {
				t.Errorf("concurrent rewind failed: %v", err)
				return
			}
		}
	}()
	for range 50 {
		pullOnce(t, source, ctx, nil)
	}
	wg.Wait()
	offset, err := source.GetOffset()
	if err != nil {
		t.Fatal(err)
	}
	if got := offset.([][]byte); len(got) > 64 {
		t.Fatalf("bounded dedup list exceeded its limit: %d", len(got))
	}
}

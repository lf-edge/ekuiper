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

	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

var _ checkpoint.ImmutableOffsetProvider = (*randomSource)(nil)

func TestDedupStateCheckpointRoundTrip(t *testing.T) {
	live := [][]byte{[]byte("first"), []byte("second")}
	frozen, err := checkpoint.EncodeState(map[string]interface{}{dedupStateKey: live})
	if err != nil {
		t.Fatal(err)
	}

	live[0][0] = 'X'
	restored, err := checkpoint.DecodeState(frozen)
	if err != nil {
		t.Fatal(err)
	}
	got := restored[dedupStateKey].([][]byte)
	if len(got) != 2 || !bytes.Equal(got[0], []byte("first")) || !bytes.Equal(got[1], []byte("second")) {
		t.Fatalf("unexpected restored dedup state: %#v", got)
	}
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
	source := &randomSource{conf: &randomSourceConfig{Deduplicate: -1}}
	next := map[string]interface{}{"value": int64(1)}

	if source.isDup(ctx, next) {
		t.Fatal("first value must not be a duplicate")
	}
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
	if source.isDup(ctx, map[string]interface{}{"value": int64(2)}) {
		t.Fatal("second value must not be a duplicate")
	}
	if len(published) != 1 || !bytes.Equal(published[0], []byte(`{"value":1}`)) {
		t.Fatalf("published offset changed after append: %#v", published)
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

	if source.isDup(ctx, map[string]interface{}{"value": int64(3)}) {
		t.Fatal("third value must not be a duplicate")
	}
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
				conf: &randomSourceConfig{Deduplicate: deduplicate},
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
			for i := range 100 {
				if source.isDup(ctx, map[string]interface{}{"value": int64(i)}) {
					t.Fatalf("new value %d must not be a duplicate", i)
				}
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

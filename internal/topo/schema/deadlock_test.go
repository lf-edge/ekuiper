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

package schema

import (
	"sync"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestSchemaRegistryDeadlock(t *testing.T) {
	// 1. Setup Global Store
	// GlobalSchemaStore is initialized by init(), but we should clean it to be safe
	GlobalSchemaStore.Lock()
	GlobalSchemaStore.streamMap = make(map[string]SchemaContainer)
	GlobalSchemaStore.schemaMap = make(map[string]map[string]map[string]*ast.JsonStreamField)
	GlobalSchemaStore.Unlock()

	streamName := "deadlock_stream_" + t.Name()

	// 2. Create SharedLayer
	// GetStream creates it if missing, and acquires Global Lock
	c := GetStream(streamName)
	s := c.(*SharedLayer)

	// Create channels to signal start
	startChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: Simulate "Plan" phase calling GetStreamSchemaIndex
	// Route: GlobalSchemaStore.RLock -> SharedLayer.RLock
	go func() {
		defer wg.Done()
		<-startChan

		for i := 0; i < 5000; i++ {
			// This used to deadlock: Global RLock -> Shared RLock
			GetStreamSchemaIndex(streamName)
		}
	}()

	// Goroutine 2: Simulate "Open" calling Attach
	// Route: SharedLayer.Lock -> updateReg -> AddRuleSchema -> GlobalSchemaStore.Lock
	go func() {
		defer wg.Done()

		// Pre-register rule schema so Attach has something to update
		ruleID := "rule_deadlock"
		s.RegSchema(ruleID, "datasource", nil, false)

		// Use standard mock context
		ctx := mockContext.NewMockContext(ruleID, "op1")

		<-startChan
		for i := 0; i < 5000; i++ {
			// This used to deadlock: Shared Lock -> Global Lock
			s.Attach(ctx)
		}
	}()

	// Run test
	close(startChan)

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Test finished successfully - No Deadlock")
	case <-time.After(10 * time.Second):
		t.Fatal("Test Timeout! Deadlock Detected!")
	}
}

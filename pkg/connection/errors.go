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

import "errors"

var (
	// ErrConnectionClosed reports that a waiter was woken by connection
	// lifecycle termination (Drop/Update/zero-ref/shutdown) rather than
	// by its own caller context. It must never be confused with
	// caller cancellation (ctx.Err()).
	ErrConnectionClosed = errors.New("connection lifecycle closed")
	// ErrConnectionRemoving reports that the requested key is owned by
	// teardown. Callers apply operation-specific policy: RequireExisting
	// fetches and named creates fail fast with it, while anonymous
	// fetches wait for cleanup and retry the lookup.
	ErrConnectionRemoving = errors.New("connection is being removed")
)

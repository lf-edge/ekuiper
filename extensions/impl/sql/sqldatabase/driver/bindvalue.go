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

package driver

import "strings"

// This file is intentionally free of build tags: it holds the bind-value
// transformer registry used by the generic SQL sink/lookup, while the
// per-driver converters live in the build-tagged driver files next to the
// driver imports they depend on. Builds excluding a driver simply have no
// transformer registered for it.
var bindTransformers = map[string]func(any) any{}

// registerBindTransformer registers a bind-value converter for a driver
// name as reported by dburl. Called from init() in build-tagged driver
// files. Unexported so registration can only happen at init time.
func registerBindTransformer(driver string, fn func(any) any) {
	bindTransformers[strings.ToLower(driver)] = fn
}

// TransformerFor returns the registered converter for a driver, or nil when
// the driver needs none (values pass through unchanged).
func TransformerFor(driver string) func(any) any {
	return bindTransformers[strings.ToLower(driver)]
}

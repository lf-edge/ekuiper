// Copyright 2024 EMQ Technologies Co., Ltd.
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

package api

type ModuleInfo struct {
	Id          string
	Description string
	New         func() Nodelet
}

type Nodelet interface {
	// Provision is called when the node is created, usually setting the configs. Do not put time-consuming operations here.
	Provision(ctx StreamContext, configs map[string]any) error
	Closable
	// Info() *ModuleInfo
	// Validate(ctx StreamContext) error
}

type Closable interface {
	Close(ctx StreamContext) error
}

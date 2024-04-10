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

package contract

// Sink is the interface that wraps the basic Sink method.
// It is used to connect to the external system and send data to it.
// A sink must implement the Sink interface AND any collector interface.
type Sink interface {
	Node
	Connector
}

type BytesCollector interface {
	Collect(ctx StreamContext, item []byte) error
}

type MessageCollector interface {
	Collect(ctx StreamContext, item ReadonlyMessage) error
}
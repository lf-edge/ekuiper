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

package node

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type Emitter interface {
	AddOutput(chan any, string) error
	RemoveOutput(string) error
}

type Collector interface {
	GetInput() (chan any, string)
}

type TopNode interface {
	GetName() string
}

// CompNode is a composite node. For implicit splitted nodes
// For example, sink node or source node may be implemented internally as a collection of connected nodes
type CompNode interface {
	TopNode
	Nodes() []TopNode
}

type MetricNode interface {
	GetMetrics() []any
	RemoveMetrics(ruleId string)
}

type OperatorNode interface {
	DataSinkNode
	Emitter
	Broadcast(data interface{})
}

type DataSourceNode interface {
	TopNode
	MetricNode
	Emitter
	Open(ctx api.StreamContext, errCh chan<- error)
	SetupFinNotify(<-chan struct{})
}

type DataSinkNode interface {
	TopNode
	MetricNode
	Collector
	Exec(api.StreamContext, chan<- error)
	GetStreamContext() api.StreamContext
	GetInputCount() int
	AddInputCount()
	SetQos(def.Qos)
	SetBarrierHandler(checkpoint.BarrierHandler)
}

type SourceInstanceNode interface {
	GetSource() api.Source
}

type MergeableTopo interface {
	GetSource() DataSourceNode
	// MergeSrc Add child topo as the source with following operators
	MergeSrc(parentTopo *def.PrintableTopo)
	// LinkTopo Add printable topo link from the parent topo to the child topo
	LinkTopo(parentTopo *def.PrintableTopo, parentJointName string)
	// SubMetrics return the metrics of the sub nodes
	SubMetrics() ([]string, []any)
	// Close notifies subtopo to deref
	Close(ctx api.StreamContext, ruleId string, runId int)
}

type SchemaNode interface {
	// AttachSchema attach the schema to the node. The parameters are ruleId, sourceName, schema, whether is wildcard
	AttachSchema(api.StreamContext, string, map[string]*ast.JsonStreamField, bool)
	// DetachSchema detach the schema from the node. The parameters are ruleId
	DetachSchema(api.StreamContext, string)
}

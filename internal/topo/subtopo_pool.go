// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package topo

import (
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
)

var (
	subTopoPool = make(map[string]*SrcSubTopo)
	lock        sync.Mutex
)

func GetOrCreateSubTopo(ctx api.StreamContext, name string) (*SrcSubTopo, bool) {
	lock.Lock()
	defer lock.Unlock()
	ac, ok := subTopoPool[name]
	if !ok {
		ac = &SrcSubTopo{
			name: name,
			topo: &def.PrintableTopo{
				Sources: make([]string, 0),
				Edges:   make(map[string][]any),
			},
			schemaReg: make(map[string]schemainfo),
			refRules:  make(map[string]chan<- error),
		}
		subTopoPool[name] = ac
	}
	// shared connection can create without reference, so the ctx may be nil
	if ctx != nil {
		ac.AddRef(ctx, nil)
	}
	return ac, ok
}

func RemoveSubTopo(name string) {
	lock.Lock()
	defer lock.Unlock()
	delete(subTopoPool, name)
	conf.Log.Infof("Delete SubTopo %s", name)
}

func (s *SrcSubTopo) AddSrc(src node.DataSourceNode) *SrcSubTopo {
	s.source = src
	switch rt := src.(type) {
	case node.MergeableTopo:
		rt.MergeSrc(s.topo)
	default:
		s.topo.Sources = append(s.topo.Sources, fmt.Sprintf("source_%s", src.GetName()))
	}
	s.tail = src
	return s
}

// AddOperator adds an internal operator to the subtopo.
func (s *SrcSubTopo) AddOperator(inputs []node.Emitter, operator node.OperatorNode) *SrcSubTopo {
	for _, input := range inputs {
		input.AddOutput(operator.GetInput())
		operator.AddInputCount()
		switch rt := input.(type) {
		case node.MergeableTopo:
			rt.LinkTopo(s.topo, s.name+"_"+operator.GetName())
		case node.TopNode:
			s.addEdge(rt, operator, "op")
		}
	}
	s.ops = append(s.ops, operator)
	s.tail = operator
	return s
}

func (s *SrcSubTopo) addEdge(from node.TopNode, to node.TopNode, toType string) {
	var f string
	switch from.(type) {
	case node.DataSourceNode:
		f = fmt.Sprintf("source_%s", from.GetName())
	default:
		f = fmt.Sprintf("op_%s_%s", s.name, from.GetName())
	}
	t := fmt.Sprintf("%s_%s_%s", toType, s.name, to.GetName())
	e, ok := s.topo.Edges[f]
	if !ok {
		e = make([]interface{}, 0)
	}
	s.topo.Edges[f] = append(e, t)
}

func (s *SrcSubTopo) MergeSrc(parentTopo *def.PrintableTopo) {
	parentTopo.Sources = append(parentTopo.Sources, s.topo.Sources...)
	for k, v := range s.topo.Edges {
		parentTopo.Edges[k] = v
	}
}

func (s *SrcSubTopo) LinkTopo(parentTopo *def.PrintableTopo, parentJointName string) {
	if _, ok := s.tail.(node.DataSourceNode); ok {
		parentTopo.Edges[fmt.Sprintf("source_%s", s.tail.(node.TopNode).GetName())] = []any{fmt.Sprintf("op_%s", parentJointName)}
	} else {
		parentTopo.Edges[fmt.Sprintf("op_%s_%s", s.name, s.tail.(node.TopNode).GetName())] = []any{fmt.Sprintf("op_%s", parentJointName)}
	}
}

var _ node.MergeableTopo = &SrcSubTopo{}

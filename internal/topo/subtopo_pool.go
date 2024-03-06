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

package topo

import (
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/pkg/api"
)

var subTopoPool = sync.Map{}

func GetSubTopo(name string) (*SrcSubTopo, bool) {
	ac, ok := subTopoPool.LoadOrStore(name, &SrcSubTopo{
		name: name,
		topo: &api.PrintableTopo{
			Sources: make([]string, 0),
			Edges:   make(map[string][]any),
		},
		schemaReg: make(map[string]schemainfo),
	})
	return ac.(*SrcSubTopo), ok
}

func RemoveSubTopo(name string) {
	subTopoPool.Delete(name)
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
func (s *SrcSubTopo) AddOperator(inputs []api.Emitter, operator node.OperatorNode) *SrcSubTopo {
	for _, input := range inputs {
		input.AddOutput(operator.GetInput())
		operator.AddInputCount()
		switch rt := input.(type) {
		case node.MergeableTopo:
			rt.LinkTopo(s.topo, s.name+"_"+operator.GetName())
		case api.TopNode:
			s.addEdge(rt, operator, "op")
		}
	}
	s.ops = append(s.ops, operator)
	s.tail = operator
	return s
}

func (s *SrcSubTopo) addEdge(from api.TopNode, to api.TopNode, toType string) {
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

func (s *SrcSubTopo) MergeSrc(parentTopo *api.PrintableTopo) {
	parentTopo.Sources = append(parentTopo.Sources, s.topo.Sources...)
	for k, v := range s.topo.Edges {
		parentTopo.Edges[k] = v
	}
}

func (s *SrcSubTopo) LinkTopo(parentTopo *api.PrintableTopo, parentJointName string) {
	if _, ok := s.tail.(node.DataSourceNode); ok {
		parentTopo.Edges[fmt.Sprintf("source_%s", s.tail.(api.TopNode).GetName())] = []any{fmt.Sprintf("op_%s", parentJointName)}
	} else {
		parentTopo.Edges[fmt.Sprintf("op_%s_%s", s.name, s.tail.(api.TopNode).GetName())] = []any{fmt.Sprintf("op_%s", parentJointName)}
	}
}

var _ node.MergeableTopo = &SrcSubTopo{}

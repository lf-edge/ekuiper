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

package planner

import (
	"fmt"
	"regexp"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// SinkPlanner is the planner for sink node. It transforms logical sink plan to multiple physical nodes.
// It will split the sink plan into multiple sink nodes according to its sink configurations.

func buildActions(tp *topo.Topo, rule *def.Rule, inputs []node.Emitter, streamCount int, schema map[string]*ast.JsonStreamField) error {
	for i, m := range rule.Actions {
		for name, action := range m {
			props, ok := action.(map[string]any)
			if !ok {
				return fmt.Errorf("expect map[string]interface{} type for the action properties, but found %v", action)
			}
			props, err := conf.OverwriteByConnectionConf(name, props)
			if err != nil {
				return err
			}
			sinkName := fmt.Sprintf("%s_%d", name, i)
			cn, err := SinkToComp(tp, name, sinkName, copyProps(props), rule, streamCount, schema)
			if err != nil {
				return err
			}
			PlanSinkOps(tp, inputs, cn)
		}
	}
	return nil
}

func copyProps(raw map[string]any) map[string]any {
	newProps := make(map[string]any, len(raw))
	for k, v := range raw {
		newProps[k] = v
	}
	return newProps
}

func PlanSinkOps(tp *topo.Topo, inputs []node.Emitter, cn node.CompNode) {
	newInputs := inputs
	var preSink node.DataSinkNode
	for _, n := range cn.Nodes() {
		switch nt := n.(type) {
		// The case order is important, because sink node is also operator node
		case *node.SinkNode:
			preSink = nt
			tp.AddSink(newInputs, nt)
		case node.OperatorNode:
			if preSink != nil { // resend
				tp.AddSinkAlterOperator(preSink.(*node.SinkNode), nt)
				preSink = nil
			} else {
				tp.AddOperator(newInputs, nt)
			}
			newInputs = []node.Emitter{nt}
		}
	}
}

func SinkToComp(tp *topo.Topo, sinkType string, sinkName string, props map[string]any, rule *def.Rule, streamCount int, schema map[string]*ast.JsonStreamField) (node.CompNode, error) {
	s, _ := io.Sink(sinkType)
	if s == nil {
		return nil, fmt.Errorf("sink %s is not defined", sinkType)
	}
	if err := s.Provision(tp.GetContext(), props); err != nil {
		return nil, err
	}
	tp.GetContext().GetLogger().Infof("provision sink %s with props %+v", sinkName, props)
	// consume common conf in sink itself, swallow and not pass it to common conf
	if pc, ok := s.(model.PropsConsumer); ok {
		pc.Consume(props)
	}
	commonConf, err := node.ParseConf(tp.GetContext().GetLogger(), props)
	if err != nil {
		return nil, fmt.Errorf("fail to parse sink configuration: %v", err)
	}
	templates := findTemplateProps(props)
	// Split sink node
	sinkOps, err := splitSink(tp, s, sinkName, rule.Options, commonConf, templates, schema)
	if err != nil {
		return nil, err
	}

	result := &SinkCompNode{
		name:  sinkName,
		nodes: sinkOps,
	}
	var snk node.DataSinkNode
	switch ss := s.(type) {
	case api.BytesCollector:
		snk, err = node.NewBytesSinkNode(tp.GetContext(), sinkName, ss, *rule.Options, streamCount, commonConf, false)
	case api.TupleCollector:
		snk, err = node.NewTupleSinkNode(tp.GetContext(), sinkName, ss, *rule.Options, streamCount, commonConf, false)
	default:
		err = fmt.Errorf("sink type %s does not implement any collector", sinkType)
	}
	if err != nil {
		return nil, err
	}
	result.nodes = append(result.nodes, snk)
	// Cache in alter queue, the topo becomes sink (fail) -> cache -> resendSink
	// If no alter queue, the topo is cache -> sink
	if commonConf.EnableCache && commonConf.ResendAlterQueue {
		s, _ := io.Sink(sinkType)
		// TODO currently, the destination prop must be named topic
		if commonConf.ResendDestination != "" {
			props["topic"] = commonConf.ResendDestination
		}
		if err = s.Provision(tp.GetContext(), props); err != nil {
			return nil, err
		}
		tp.GetContext().GetLogger().Infof("provision sink %s with props %+v", sinkName, props)

		cacheOp, err := node.NewCacheOp(tp.GetContext(), fmt.Sprintf("%s_cache", sinkName), rule.Options, &commonConf.SinkConf)
		if err != nil {
			return nil, err
		}
		result.nodes = append(result.nodes, cacheOp)

		sinkName := fmt.Sprintf("%s_resend", sinkName)
		var snk node.DataSinkNode
		switch ss := s.(type) {
		case api.BytesCollector:
			snk, err = node.NewBytesSinkNode(tp.GetContext(), sinkName, ss, *rule.Options, streamCount, commonConf, true)
		case api.TupleCollector:
			snk, err = node.NewTupleSinkNode(tp.GetContext(), sinkName, ss, *rule.Options, streamCount, commonConf, true)
		default:
			err = fmt.Errorf("sink type %s does not implement any collector", sinkType)
		}
		if err != nil {
			return nil, err
		}
		result.nodes = append(result.nodes, snk)
	}
	return result, nil
}

func findTemplateProps(props map[string]any) []string {
	var result []string
	re := regexp.MustCompile(`{{(.*?)}}`)
	for _, p := range props {
		switch pt := p.(type) {
		case string:
			if re.Match([]byte(pt)) {
				result = append(result, pt)
			}
		case map[string]any:
			res := findTemplateProps(pt)
			result = append(result, res...)
		}
	}
	return result
}

// Split sink node according to the sink configuration. Return the new input emitters.
func splitSink(tp *topo.Topo, s api.Sink, sinkName string, options *def.RuleOption, sc *node.SinkConf, templates []string, schema map[string]*ast.JsonStreamField) ([]node.TopNode, error) {
	// tailor schema, each sink may have different transform field
	schema = washSchema(sc, schema)
	index := 0
	result := make([]node.TopNode, 0)
	var sinkInfo model.SinkInfo
	switch st := s.(type) {
	case model.SinkInfoNode:
		sinkInfo = st.Info()
	default:
		sinkInfo = model.SinkInfo{}
	}
	batchEnabled := !sinkInfo.HasBatch && (sc.BatchSize > 0 || sc.LingerInterval > 0)
	// Batch enabled
	if batchEnabled {
		batchOp, err := node.NewBatchOp(fmt.Sprintf("%s_%d_batch", sinkName, index), options, sc.BatchSize, time.Duration(sc.LingerInterval))
		if err != nil {
			return nil, err
		}
		index++
		result = append(result, batchOp)
	}
	// Transform enabled
	// Currently, the row to map is done here and is required. TODO: eliminate map and this could become optional
	transformOp, err := node.NewTransformOp(fmt.Sprintf("%s_%d_transform", sinkName, index), options, sc, templates)
	if err != nil {
		return nil, err
	}
	index++
	result = append(result, transformOp)
	if batchEnabled {
		batchWriterOp, err := node.NewBatchWriterOp(tp.GetContext(), fmt.Sprintf("%s_%d_batchWriter", sinkName, index), options, schema, sc)
		if err != nil {
			return nil, err
		}
		index++
		result = append(result, batchWriterOp)
	}
	// Encode will convert the result to []byte
	if _, ok := s.(api.BytesCollector); ok {
		if !batchEnabled {
			encodeOp, err := node.NewEncodeOp(tp.GetContext(), fmt.Sprintf("%s_%d_encode", sinkName, index), options, schema, sc)
			if err != nil {
				return nil, err
			}
			index++
			result = append(result, encodeOp)
		}

		_, isStreamWriter := s.(model.StreamWriter)
		if !sinkInfo.HasCompress && !isStreamWriter && sc.Compression != "" {
			compressOp, err := node.NewCompressOp(fmt.Sprintf("%s_%d_compress", sinkName, index), options, sc.Compression, sc.CompressionProps)
			if err != nil {
				return nil, err
			}
			index++
			result = append(result, compressOp)
		}

		if !isStreamWriter && sc.Encryption != "" {
			encryptOp, err := node.NewEncryptOp(fmt.Sprintf("%s_%d_encrypt", sinkName, index), options, sc.Encryption, sc.EncProps)
			if err != nil {
				return nil, err
			}
			index++
			result = append(result, encryptOp)
		}
	}
	// Caching
	if sc.EnableCache && !sc.ResendAlterQueue {
		cacheOp, err := node.NewCacheOp(tp.GetContext(), fmt.Sprintf("%s_%d_cache", sinkName, index), options, &sc.SinkConf)
		if err != nil {
			return nil, err
		}
		index++
		result = append(result, cacheOp)
	}
	return result, nil
}

func washSchema(sc *node.SinkConf, schema map[string]*ast.JsonStreamField) map[string]*ast.JsonStreamField {
	if sc.DataField != "" || len(sc.Fields) > 0 || len(sc.ExcludeFields) > 0 {
		washedSchema := make(map[string]*ast.JsonStreamField)
		if len(sc.Fields) > 0 {
			for _, field := range sc.Fields {
				washedSchema[field] = schema[field]
			}
		} else if sc.DataField == "" && len(sc.ExcludeFields) > 0 {
			excludedMap := make(map[string]struct{}, len(sc.ExcludeFields))
			for _, field := range sc.ExcludeFields {
				excludedMap[field] = struct{}{}
			}
			for k, v := range schema {
				if _, ok := excludedMap[k]; ok {
					continue
				}
				washedSchema[k] = v
			}
		} else { // set DataField, we don't know the final schema then
			washedSchema = nil
		}
		schema = washedSchema
	}
	return schema
}

type SinkCompNode struct {
	name  string
	nodes []node.TopNode
}

func (s *SinkCompNode) GetName() string {
	return s.name
}

func (s *SinkCompNode) Nodes() []node.TopNode {
	return s.nodes
}

var _ node.CompNode = &SinkCompNode{}

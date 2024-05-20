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
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// SinkPlanner is the planner for sink node. It transforms logical sink plan to multiple physical nodes.
// It will split the sink plan into multiple sink nodes according to its sink configurations.

func buildActions(tp *topo.Topo, rule *def.Rule, inputs []node.Emitter, streamCount int) error {
	for i, m := range rule.Actions {
		for name, action := range m {
			s, _ := io.Sink(name)
			if s == nil {
				return fmt.Errorf("sink %s is not defined", name)
			}
			props, ok := action.(map[string]any)
			if !ok {
				return fmt.Errorf("expect map[string]interface{} type for the action properties, but found %v", action)
			}
			commonConf, err := node.ParseConf(tp.GetContext().GetLogger(), props)
			if err != nil {
				return fmt.Errorf("fail to parse sink configuration: %v", err)
			}
			templates := findTemplateProps(props)
			// Split sink node
			sinkName := fmt.Sprintf("%s_%d", name, i)
			newInputs, err := splitSink(tp, inputs, s, sinkName, rule.Options, commonConf, templates)
			if err != nil {
				return err
			}
			if err = s.Provision(tp.GetContext(), props); err != nil {
				return err
			}
			tp.GetContext().GetLogger().Infof("provision sink %s with props %+v", sinkName, props)

			var snk node.DataSinkNode
			switch ss := s.(type) {
			case api.BytesCollector:
				snk, err = node.NewBytesSinkNode(tp.GetContext(), sinkName, ss, *rule.Options, streamCount, &commonConf.SinkConf, false)
			case api.TupleCollector:
				snk, err = node.NewTupleSinkNode(tp.GetContext(), sinkName, ss, *rule.Options, streamCount, &commonConf.SinkConf, false)
			default:
				err = fmt.Errorf("sink type %s does not implement any collector", name)
			}
			if err != nil {
				return err
			}
			tp.AddSink(newInputs, snk)
			// Cache in alter queue, the topo becomes sink (fail) -> cache -> resendSink
			// If no alter queue, the topo is cache -> sink
			if commonConf.EnableCache && commonConf.ResendAlterQueue {
				s, _ := io.Sink(name)
				// TODO currently, the destination prop must be named topic
				if commonConf.ResendDestination != "" {
					props["topic"] = commonConf.ResendDestination
				}
				if err = s.Provision(tp.GetContext(), props); err != nil {
					return err
				}
				tp.GetContext().GetLogger().Infof("provision sink %s with props %+v", sinkName, props)

				cacheOp, err := node.NewCacheOp(tp.GetContext(), fmt.Sprintf("%s_cache", sinkName), rule.Options, &commonConf.SinkConf)
				if err != nil {
					return err
				}
				tp.AddSinkAlterOperator(snk.(*node.SinkNode), cacheOp)
				newInputs = []node.Emitter{cacheOp}

				sinkName := fmt.Sprintf("%s_resend_%d", name, i)
				var snk node.DataSinkNode
				switch ss := s.(type) {
				case api.BytesCollector:
					snk, err = node.NewBytesSinkNode(tp.GetContext(), sinkName, ss, *rule.Options, streamCount, &commonConf.SinkConf, true)
				case api.TupleCollector:
					snk, err = node.NewTupleSinkNode(tp.GetContext(), sinkName, ss, *rule.Options, streamCount, &commonConf.SinkConf, true)
				default:
					err = fmt.Errorf("sink type %s does not implement any collector", name)
				}
				if err != nil {
					return err
				}
				tp.AddSink(newInputs, snk)
			}
		}
	}
	return nil
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
func splitSink(tp *topo.Topo, inputs []node.Emitter, s api.Sink, sinkName string, options *def.RuleOption, sc *node.SinkConf, templates []string) ([]node.Emitter, error) {
	index := 0
	newInputs := inputs
	// Batch enabled
	if sc.BatchSize > 0 || sc.LingerInterval > 0 {
		batchOp, err := node.NewBatchOp(fmt.Sprintf("%s_%d_batch", sinkName, index), options, sc.BatchSize, time.Duration(sc.LingerInterval))
		if err != nil {
			return nil, err
		}
		index++
		tp.AddOperator(newInputs, batchOp)
		newInputs = []node.Emitter{batchOp}
	}
	// Transform enabled
	// Currently, the row to map is done here and is required. TODO: eliminate map and this could become optional
	transformOp, err := node.NewTransformOp(fmt.Sprintf("%s_%d_transform", sinkName, index), options, sc, templates)
	if err != nil {
		return nil, err
	}
	index++
	tp.AddOperator(newInputs, transformOp)
	newInputs = []node.Emitter{transformOp}
	// Encode will convert the result to []byte
	if _, ok := s.(api.BytesCollector); ok {
		encodeOp, err := node.NewEncodeOp(fmt.Sprintf("%s_%d_encode", sinkName, index), options, sc)
		if err != nil {
			return nil, err
		}
		index++
		tp.AddOperator(newInputs, encodeOp)
		newInputs = []node.Emitter{encodeOp}
		_, isStreamWriter := s.(model.StreamWriter)
		if !isStreamWriter && sc.Compression != "" {
			compressOp, err := node.NewCompressOp(fmt.Sprintf("%s_%d_compress", sinkName, index), options, sc.Compression)
			if err != nil {
				return nil, err
			}
			index++
			tp.AddOperator(newInputs, compressOp)
			newInputs = []node.Emitter{compressOp}
		}

		if !isStreamWriter && sc.Encryption != "" {
			encryptOp, err := node.NewEncryptOp(fmt.Sprintf("%s_%d_encrypt", sinkName, index), options, sc.Encryption)
			if err != nil {
				return nil, err
			}
			index++
			tp.AddOperator(newInputs, encryptOp)
			newInputs = []node.Emitter{encryptOp}
		}
	}
	// Caching
	if sc.EnableCache && !sc.ResendAlterQueue {
		cacheOp, err := node.NewCacheOp(tp.GetContext(), fmt.Sprintf("%s_%d_cache", sinkName, index), options, &sc.SinkConf)
		if err != nil {
			return nil, err
		}
		index++
		tp.AddOperator(newInputs, cacheOp)
		newInputs = []node.Emitter{cacheOp}
	}
	return newInputs, nil
}

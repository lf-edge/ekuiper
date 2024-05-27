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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	nodeConf "github.com/lf-edge/ekuiper/v2/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/operator"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func transformSourceNode(ctx api.StreamContext, t *DataSourcePlan, mockSourcesProp map[string]map[string]any, ruleId string, options *def.RuleOption, index int) (node.DataSourceNode, []node.OperatorNode, int, error) {
	isSchemaless := t.isSchemaless
	_, isMock := mockSourcesProp[string(t.name)]
	if isMock {
		t.streamStmt.Options.TYPE = "simulator"
	}
	switch t.streamStmt.StreamType {
	case ast.TypeStream:
		strType := t.streamStmt.Options.TYPE
		if strType == "" {
			strType = "mqtt"
			t.streamStmt.Options.TYPE = strType
		}
		si, err := io.Source(strType)
		if err != nil {
			return nil, nil, 0, err
		}
		if si == nil {
			return nil, nil, 0, fmt.Errorf("source type %s not found", strType)
		}
		var pp node.UnOperation
		if t.iet || (!isSchemaless && (t.streamStmt.Options.STRICT_VALIDATION || t.isBinary)) {
			pp, err = operator.NewPreprocessor(isSchemaless, t.streamFields, t.allMeta, t.metaFields, t.iet, t.timestampField, t.timestampFormat, t.isBinary, t.streamStmt.Options.STRICT_VALIDATION)
			if err != nil {
				return nil, nil, 0, err
			}
		}
		return splitSource(ctx, t, si, options, index, ruleId, pp)
	case ast.TypeTable:
		si, err := io.Source(t.streamStmt.Options.TYPE)
		if err != nil {
			return nil, nil, 0, err
		}
		pp, err := operator.NewTableProcessor(isSchemaless, string(t.name), t.streamFields, t.streamStmt.Options)
		if err != nil {
			return nil, nil, 0, err
		}
		return splitSource(ctx, t, si, options, index, ruleId, pp)
	}
	return nil, nil, 0, fmt.Errorf("unknown stream type %d", t.streamStmt.StreamType)
}

type SourcePropsForSplit struct {
	Decompression string `json:"decompression"`
	SelId         string `json:"connectionSelector"`
}

func splitSource(ctx api.StreamContext, t *DataSourcePlan, ss api.Source, options *def.RuleOption, index int, ruleId string, pp node.UnOperation) (node.DataSourceNode, []node.OperatorNode, int, error) {
	// Get all props
	props := nodeConf.GetSourceConf(t.streamStmt.Options.TYPE, t.streamStmt.Options)
	sp := &SourcePropsForSplit{}
	_ = cast.MapToStruct(props, sp)
	// Create the connector node as source node
	var (
		err         error
		srcConnNode node.DataSourceNode
	)
	if sp.SelId == "" {
		srcConnNode, err = node.NewSourceNode(ctx, string(t.name), ss, props, options)
	} else { // connection selector is set as a one node sub_topo
		selName := fmt.Sprintf("%s/%s", sp.SelId, t.streamStmt.Options.DATASOURCE)
		srcSubtopo, existed := topo.GetSubTopo(selName)
		if !existed {
			var scn node.DataSourceNode
			scn, err = node.NewSourceNode(ctx, selName, ss, props, options)
			if err == nil {
				ctx.GetLogger().Infof("Create SubTopo %s for shared connection", selName)
				srcSubtopo.AddSrc(scn)
			}
		}
		srcConnNode = srcSubtopo
	}
	// Make sure it is provisioned. Already done in NewSourceNode
	info := checkByteSource(ss)
	if info.HasCompress {
		sp.Decompression = ""
	}
	if info.HasInterval {
		delete(props, "sendInterval")
	}

	if err != nil {
		return nil, nil, 0, err
	}
	index++
	var ops []node.OperatorNode

	if sp.Decompression != "" {
		if info.NeedBatchDecode {
			dco, err := node.NewDecompressOp(fmt.Sprintf("%d_decompress", index), options, sp.Decompression)
			if err != nil {
				return nil, nil, 0, err
			}
			index++
			ops = append(ops, dco)
		} else {
			ctx.GetLogger().Warnf("source %s does not support decompression", t.name)
		}
	}

	if info.NeedDecode {
		// Create the decode node
		decodeNode, err := node.NewDecodeOp(ctx, false, fmt.Sprintf("%d_decoder", index), string(t.streamStmt.Name), ruleId, options, t.streamStmt.Options, t.isWildCard, t.isSchemaless, t.streamFields, props)
		if err != nil {
			return nil, nil, 0, err
		}
		index++
		ops = append(ops, decodeNode)
	}

	// Create the preprocessor node if needed
	if pp != nil {
		ops = append(ops, Transform(pp, fmt.Sprintf("%d_preprocessor", index), options))
		index++
	}

	if t.streamStmt.Options.SHARED && len(ops) > 0 {
		// Create subtopo in the end to avoid errors in the middle
		srcSubtopo, existed := topo.GetSubTopo(string(t.name))
		if !existed {
			ctx.GetLogger().Infof("Create SubTopo %s", string(t.name))
			srcSubtopo.AddSrc(srcConnNode)
			subInputs := []node.Emitter{srcSubtopo}
			for _, e := range ops {
				srcSubtopo.AddOperator(subInputs, e)
				subInputs = []node.Emitter{e}
			}
		}
		srcSubtopo.StoreSchema(ruleId, string(t.name), t.streamFields, t.isWildCard)
		return srcSubtopo, nil, len(ops), nil
	}
	return srcConnNode, ops, 0, nil
}

func checkByteSource(ss api.Source) model.NodeInfo {
	switch st := ss.(type) {
	case model.InfoNode:
		return st.Info()
	case api.BytesSource:
		return model.NodeInfo{
			NeedDecode:      true,
			NeedBatchDecode: true,
		}
	default:
		return model.NodeInfo{}
	}
}

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
	"time"

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
	if err != nil {
		return nil, nil, 0, err
	}
	index++
	var ops []node.OperatorNode

	// Need to check after source has provisioned, so do not put it before provision
	featureSet, err := checkFeatures(ss, sp, props)
	if err != nil {
		return nil, nil, 0, err
	}

	if featureSet.needRatelimit {
		rlOp, err := node.NewRateLimitOp(ctx, fmt.Sprintf("%d_ratelimit", index), options, props)
		if err != nil {
			return nil, nil, 0, err
		}
		index++
		ops = append(ops, rlOp)
	}

	if featureSet.needCompression {
		dco, err := node.NewDecompressOp(fmt.Sprintf("%d_decompress", index), options, sp.Decompression)
		if err != nil {
			return nil, nil, 0, err
		}
		index++
		ops = append(ops, dco)
	}

	if featureSet.needDecode {
		// Create the decode node
		decodeNode, err := node.NewDecodeOp(ctx, false, fmt.Sprintf("%d_decoder", index), string(t.streamStmt.Name), ruleId, options, t.streamStmt.Options, t.isWildCard, t.isSchemaless, t.streamFields, props)
		if err != nil {
			return nil, nil, 0, err
		}
		index++
		ops = append(ops, decodeNode)
	}

	if featureSet.needPayloadDecode {
		// Create the decode node
		payloadDecodeNode, err := node.NewDecodeOp(ctx, true, fmt.Sprintf("%d_payload_decoder", index), string(t.streamStmt.Name), ruleId, options, t.streamStmt.Options, t.isWildCard, t.isSchemaless, t.streamFields, props)
		if err != nil {
			return nil, nil, 0, err
		}
		index++
		ops = append(ops, payloadDecodeNode)
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

type SourcePropsForSplit struct {
	Decompression string        `json:"decompression"`
	SelId         string        `json:"connectionSelector"`
	PayloadFormat string        `json:"payloadFormat"`
	Interval      time.Duration `json:"interval"`
	MergeField    string        `json:"mergeField"`
	Format        string        `json:"format"`
}

type traits struct {
	needConnection    bool
	needCompression   bool
	needDecode        bool
	needPayloadDecode bool
	needRatelimit     bool
}

// function to return if a sub node is needed
func checkFeatures(ss api.Source, sp *SourcePropsForSplit, props map[string]any) (traits, error) {
	info := checkByteSource(ss)
	// TODO here is a hack for file source send interval. If it is sent in file, do not need to process sendInterval in decode
	if info.HasInterval {
		delete(props, "sendInterval")
	}
	r := traits{
		needConnection:    sp.SelId != "",
		needCompression:   sp.Decompression != "" && (!info.HasCompress || info.NeedBatchDecode),
		needDecode:        info.NeedDecode,
		needPayloadDecode: sp.PayloadFormat != "",
	}
	if sp.Interval > 0 {
		switch ss.(type) {
		// pull source already pull internally which is also a rate limiter
		case api.PullTupleSource, api.PullBytesSource:
			r.needRatelimit = false
		default:
			r.needRatelimit = true
		}
	}
	if r.needRatelimit && sp.MergeField != "" && r.needDecode {
		if r.needPayloadDecode {
			return r, fmt.Errorf("do not support rate limit merge together with payloadFormat")
		}
		r.needPayloadDecode = true
		r.needDecode = false
		props["payloadFormat"] = props["format"]
		props["payloadBatchField"] = "frames"
		props["payloadField"] = "data"
	}
	return r, nil
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

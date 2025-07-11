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
	mockProps, isMock := mockSourcesProp[string(t.name)]
	if isMock {
		t.streamStmt.Options.TYPE = "simulator"
		t.inRuleTest = true
	}
	strType := t.streamStmt.Options.TYPE
	if strType == "" {
		switch t.streamStmt.StreamType {
		case ast.TypeStream:
			strType = "mqtt"
		case ast.TypeTable:
			strType = "file"
		}
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
	return splitSource(ctx, t, si, options, mockProps, index, ruleId, pp)
}

func splitSource(ctx api.StreamContext, t *DataSourcePlan, ss api.Source, options *def.RuleOption, mockProps map[string]any, index int, ruleId string, pp node.UnOperation) (node.DataSourceNode, []node.OperatorNode, int, error) {
	// Get all props
	props := nodeConf.GetSourceConf(t.streamStmt.Options.TYPE, t.streamStmt.Options)
	sp := &SourcePropsForSplit{}
	if len(mockProps) > 0 {
		for k, v := range mockProps {
			props[k] = v
		}
	}
	_ = cast.MapToStruct(props, sp)
	// Create the connector node as source node
	var (
		err         error
		srcConnNode node.DataSourceNode
	)
	// Some connection only allow one subscription. The source should implement UniqueSub to provide a subId to avoid multiple connection.
	us, hasSubId := ss.(model.UniqueSub)
	conId := sp.SelId
	// Some connection only have on
	cs, hasConId := ss.(model.UniqueConn)
	if hasConId {
		conId = cs.ConnId(props)
	}

	var ops []node.OperatorNode
	// If having unique connection id AND unique sub id for each connection, need to share the sub node; Case 1 is neuron; Case 2 is edgeX
	needShareCon := hasConId || (hasSubId && conId != "")
	if !needShareCon {
		srcConnNode, err = node.NewSourceNode(ctx, string(t.name), ss, props, options)
		if err != nil {
			return nil, nil, 0, err
		}
		index++
	} else { // connection selector is set as a one node sub_topo
		subId := t.streamStmt.Options.DATASOURCE
		if hasSubId {
			subId = us.SubId(props)
		}
		selName := fmt.Sprintf("%s/%s", conId, subId)
		srcSubtopo, existed := topo.GetOrCreateSubTopo(nil, selName)
		if !existed {
			var scn node.DataSourceNode
			scn, err = node.NewSourceNode(ctx, selName, ss, props, options)
			if err == nil {
				ctx.GetLogger().Infof("Create SubTopo %s for shared connection", selName)
				srcSubtopo.AddSrc(scn)
			}
		} else {
			ctx.GetLogger().Infof("Load SubTopo %s for shared connection", selName)
		}
		srcConnNode = srcSubtopo
		if err != nil {
			topo.RemoveSubTopo(selName)
			return nil, nil, 0, err
		}
		index++
		// another node to set emitter
		op := Transform(&operator.EmitterOp{Emitter: string(t.name)}, fmt.Sprintf("%d_emitter", index), options)
		index++
		ops = append(ops, op)
	}
	if len(t.colAliasMapping) > 0 {
		props["colAliasMapping"] = t.colAliasMapping
	}

	// Need to check after source has provisioned, so do not put it before provision
	featureSet, err := checkFeatures(ss, sp, props)
	if err != nil {
		return nil, nil, 0, err
	}

	if featureSet.needRatelimit {
		rlOp, err := node.NewRateLimitOp(ctx, fmt.Sprintf("%d_ratelimit", index), options, t.streamFields, props)
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
		schema := t.streamFields
		if t.isWildCard {
			schema = nil
		}
		// Create the decode node
		decodeNode, err := node.NewDecodeOp(ctx, false, fmt.Sprintf("%d_decoder", index), string(t.streamStmt.Name), options, schema, props)
		if err != nil {
			return nil, nil, 0, err
		}
		index++
		ops = append(ops, decodeNode)
	}

	if featureSet.needRatelimitMerge {
		rlOp, err := node.NewRateLimitOp(ctx, fmt.Sprintf("%d_ratelimit", index), options, t.streamFields, props)
		if err != nil {
			return nil, nil, 0, err
		}
		index++
		ops = append(ops, rlOp)
	}

	if featureSet.needPayloadDecode {
		schema := t.streamFields
		if t.isWildCard {
			schema = nil
		}
		// Create the decode node
		payloadDecodeNode, err := node.NewDecodeOp(ctx, true, fmt.Sprintf("%d_payload_decoder", index), string(t.streamStmt.Name), options, schema, props)
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

	if t.streamStmt.Options.SHARED && !t.inRuleTest {
		// Create subtopo in the end to avoid errors in the middle
		srcSubtopo, existed := topo.GetOrCreateSubTopo(ctx, string(t.name))
		if !existed {
			ctx.GetLogger().Infof("Create SubTopo %s", string(t.name))
			srcSubtopo.AddSrc(srcConnNode)
			subInputs := []node.Emitter{srcSubtopo}
			for _, e := range ops {
				srcSubtopo.AddOperator(subInputs, e)
				subInputs = []node.Emitter{e}
			}
		} else {
			ctx.GetLogger().Infof("Load SubTopo %s", string(t.name))
		}
		srcSubtopo.StoreSchema(ruleId, string(t.name), t.streamFields, t.isWildCard)
		return srcSubtopo, nil, len(ops), nil
	}
	return srcConnNode, ops, 0, nil
}

type SourcePropsForSplit struct {
	Decompression string            `json:"decompression"`
	SelId         string            `json:"connectionSelector"`
	PayloadFormat string            `json:"payloadFormat"`
	Interval      cast.DurationConf `json:"interval"`
	// merger and mergerField should only set one
	MergeField string `json:"mergeField"`
	Merger     string `json:"merger"`
	Format     string `json:"format"`
}

type traits struct {
	needConnection    bool
	needCompression   bool
	needDecode        bool
	needPayloadDecode bool
	// rate limit will plan right after source read
	needRatelimit bool
	// rate limit merge will plan after decompress
	needRatelimitMerge bool
}

// function to return if a sub node is needed
func checkFeatures(ss api.Source, sp *SourcePropsForSplit, props map[string]any) (traits, error) {
	// validate merger
	if sp.Merger != "" && sp.MergeField != "" {
		return traits{}, fmt.Errorf("mergeField and merger cannot set together")
	}
	// Let merger payload format defaults to format
	if sp.Merger != "" && sp.PayloadFormat == "" {
		sp.PayloadFormat = sp.Format
		if sp.PayloadFormat == "" {
			sp.PayloadFormat = "json"
		}
		props["payloadFormat"] = sp.PayloadFormat
	}

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
		props["payloadSchemaId"] = props["schemaId"]
		props["payloadDelimiter"] = props["delimiter"]
		props["payloadBatchField"] = "frames"
		props["payloadField"] = "data"
	}
	if !r.needRatelimit && sp.Merger != "" {
		return r, fmt.Errorf("merger is set but rate limit is not required")
	}
	// If rate limit merger is set, the first level decode will be done by merger
	if r.needRatelimit && sp.Merger != "" {
		r.needRatelimitMerge = true
		r.needRatelimit = false
		r.needDecode = false
		props["payloadBatchField"] = "frames"
		props["payloadField"] = "data"
		if _, ok := props["payloadSchemaId"]; !ok {
			props["payloadSchemaId"] = props["schemaId"]
			props["payloadDelimiter"] = props["delimiter"]
		}
	}
	return r, nil
}

func checkByteSource(ss api.Source) model.NodeInfo {
	switch st := ss.(type) {
	case model.InfoNode:
		return st.Info()
	case api.BytesSource, api.PullBytesSource:
		return model.NodeInfo{
			NeedDecode:      true,
			NeedBatchDecode: true,
		}
	default:
		return model.NodeInfo{}
	}
}

func planLookupSource(ctx api.StreamContext, t *LookupPlan, ruleOption *def.RuleOption) (node.Emitter, error) {
	si, err := io.LookupSource(t.options.TYPE)
	if err != nil {
		return nil, err
	}
	if si == nil {
		return nil, fmt.Errorf("lookup source type %s not found", t.options.TYPE)
	}
	props := nodeConf.GetSourceConf(t.options.TYPE, t.options)
	switch si.(type) {
	case api.LookupSource:
		return node.NewLookupNode(ctx, t.joinExpr.Name, false, t.fields, t.keys, t.joinExpr.JoinType, t.valvars, t.options, ruleOption, props)
	case api.LookupBytesSource:
		if t.options.FORMAT == "" {
			return nil, fmt.Errorf("lookup source type %s must specify format", t.options.TYPE)
		}
		return node.NewLookupNode(ctx, t.joinExpr.Name, true, t.fields, t.keys, t.joinExpr.JoinType, t.valvars, t.options, ruleOption, props)
	}
	return nil, fmt.Errorf("lookup source type %s is found but not a valid lookup source", t.options.TYPE)
}

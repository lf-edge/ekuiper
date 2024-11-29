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

package tracenode

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
)

const (
	DataKey = "data"
	RuleKey = "rule"
)

func RecordRowOrCollection(input interface{}, span trace.Span) {
	switch d := input.(type) {
	case xsql.Row:
		span.SetAttributes(attribute.String(DataKey, ToStringRow(d)))
	case api.MessageTupleList:
		if d.Len() > 0 {
			span.SetAttributes(attribute.String(DataKey, ToStringCollection(d)))
		}
	case *xsql.RawTuple:
		span.SetAttributes(attribute.String(DataKey, base64.StdEncoding.EncodeToString(d.Raw())))
	default:
		conf.Log.Errorf("RecordRowOrCollection got unexpected input type: %T", d)
	}
}

func TraceInput(ctx api.StreamContext, d any, opName string, opts ...trace.SpanStartOption) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	input, ok := d.(xsql.HasTracerCtx)
	if !ok {
		return false, nil, nil
	}
	if !checkCtxByStrategy(ctx, input.GetTracerCtx()) {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), opName, opts...)
	span.SetAttributes(attribute.String(RuleKey, ctx.GetRuleId()))
	x := topoContext.WithContext(spanCtx)
	input.SetTracerCtx(x)
	return true, x, span
}

func StartTraceBackground(ctx api.StreamContext, opName string, opts ...trace.SpanStartOption) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	if !checkCtxByStrategy(ctx, ctx) {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(context.Background(), opName, opts...)
	ruleID := ctx.GetRuleId()
	span.SetAttributes(attribute.String(RuleKey, ruleID))
	ingestCtx := topoContext.WithContext(spanCtx)
	return true, ingestCtx, span
}

func StartTraceByID(ctx api.StreamContext, parentId string, opts ...trace.SpanStartOption) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	carrier := map[string]string{
		"traceparent": parentId,
	}
	propagator := propagation.TraceContext{}
	traceCtx := propagator.Extract(context.Background(), propagation.MapCarrier(carrier))
	spanCtx, span := tracer.GetTracer().Start(traceCtx, ctx.GetOpId(), opts...)
	span.SetAttributes(attribute.String(RuleKey, ctx.GetRuleId()))
	ingestCtx := topoContext.WithContext(spanCtx)
	return true, ingestCtx, span
}

func ToStringRow(r xsql.Row) string {
	d := r.Clone().ToMap()
	b, _ := json.Marshal(d)
	return string(b)
}

func ToStringCollection(r api.MessageTupleList) string {
	var d []map[string]any
	// TODO all tuple list must be treated the same in the future. Let ToMaps work anywhere
	switch rt := r.(type) {
	case xsql.Collection:
		d = rt.Clone().ToMaps()
	case *xsql.TransformedTupleList:
		d = rt.Clone().ToMaps()
	default:
		return fmt.Sprintf("%v", rt)
	}
	b, _ := json.Marshal(d)
	return string(b)
}

func BuildTraceParentId(traceID [16]byte, spanID [8]byte) string {
	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(traceID[:]), hex.EncodeToString(spanID[:]))
}

func checkCtxByStrategy(ctx, tracerCtx api.StreamContext) bool {
	strategy := ExtractStrategy(ctx)
	switch strategy {
	case topoContext.AlwaysTraceStrategy:
		return true
	case topoContext.HeadTraceStrategy:
		return hasTraceContext(tracerCtx)
	}
	return false
}

func ExtractStrategy(ctx api.StreamContext) topoContext.TraceStrategy {
	dctx, ok := ctx.(*topoContext.DefaultContext)
	if !ok {
		return topoContext.AlwaysTraceStrategy
	}
	return dctx.GetStrategy()
}

func hasTraceContext(ctx context.Context) bool {
	spanContext := trace.SpanContextFromContext(ctx)
	return spanContext.IsValid()
}

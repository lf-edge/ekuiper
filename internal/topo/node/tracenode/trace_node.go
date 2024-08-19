package tracenode

import (
	"context"
	"encoding/json"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
)

const DataKey = "data"

func TraceRowTuple(ctx api.StreamContext, input *xsql.RawTuple, opName string) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), opName)
	x := topoContext.WithContext(spanCtx)
	return true, x, span
}

func RecordRowOrCollection(input interface{}, span trace.Span) {
	switch d := input.(type) {
	case xsql.Row:
		span.SetAttributes(attribute.String(DataKey, ToStringRow(d)))
	case xsql.Collection:
		if d.Len() > 0 {
			span.SetAttributes(attribute.String(DataKey, ToStringCollection(d)))
		}
	case *xsql.RawTuple:
		span.SetAttributes(attribute.String(DataKey, string(d.Rawdata)))
	}
}

func RecordSpanData(input any, span trace.Span) {
	switch d := input.(type) {
	case []byte:
		span.SetAttributes(attribute.String(DataKey, string(d)))
	}
}

func TraceInput(ctx api.StreamContext, d interface{}, opName string, opts ...trace.SpanStartOption) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	input, ok := d.(xsql.HasTracerCtx)
	if !ok {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), opName, opts...)
	x := topoContext.WithContext(spanCtx)
	input.SetTracerCtx(x)
	return true, x, span
}

func TraceRow(ctx api.StreamContext, input xsql.Row, opName string, opts ...trace.SpanStartOption) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(input.GetTracerCtx(), opName, opts...)
	x := topoContext.WithContext(spanCtx)
	input.SetTracerCtx(x)
	return true, x, span
}

func StartTrace(ctx api.StreamContext, opName string) (bool, api.StreamContext, trace.Span) {
	if !ctx.IsTraceEnabled() {
		return false, nil, nil
	}
	spanCtx, span := tracer.GetTracer().Start(context.Background(), opName)
	ingestCtx := topoContext.WithContext(spanCtx)
	ingestCtx.IsTraceEnabled()
	return true, ingestCtx, span
}

func ToStringRow(r xsql.Row) string {
	d := r.Clone().ToMap()
	b, _ := json.Marshal(d)
	return string(b)
}

func ToStringCollection(r xsql.Collection) string {
	d := r.Clone().ToMaps()
	b, _ := json.Marshal(d)
	return string(b)
}

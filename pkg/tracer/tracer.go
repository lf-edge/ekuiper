// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package tracer

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

var tracerSet = false

func InitTracer() error {
	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("kuiperd-service"),
	)))
	if conf.Config.OpenTelemetry.EnableCollector {
		ctx := context.Background()
		exporter, err := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(conf.Config.OpenTelemetry.Endpoint),
			otlptracehttp.WithInsecure(),
		)
		if err != nil {
			return err
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	tracerSet = true
	return nil
}

// only used in unit test
func initTracer() {
	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("kuiperd-service"),
	)))
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	tracerSet = true
}

func GetTracer() trace.Tracer {
	if !tracerSet {
		initTracer()
	}
	return otel.GetTracerProvider().Tracer("kuiperd-service")
}

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

//go:build trace || !core

package tracer

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

const (
	TraceCfgKey = "$$tracer_cfg"
)

var globalTracerManager *GlobalTracerManager

func init() {
	globalTracerManager = &GlobalTracerManager{}
}

type GlobalTracerManager struct {
	syncx.RWMutex
	Init                 bool
	ServiceName          string
	EnableRemoteEndpoint bool
	RemoteEndpoint       string
	SpanExporter         *SpanExporter
}

func (g *GlobalTracerManager) InitIfNot() {
	g.Lock()
	defer g.Unlock()
	if g.Init {
		return
	}
	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("kuiperd-service"),
	)))
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	g.Init = true
}

func (g *GlobalTracerManager) SetTracer(enableRemote bool, serviceName, endpoint string) error {
	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
	)))
	g.Lock()
	defer g.Unlock()
	g.ServiceName = serviceName
	g.EnableRemoteEndpoint = enableRemote
	g.RemoteEndpoint = endpoint
	exporter, err := NewSpanExporter(enableRemote, endpoint)
	if err != nil {
		return err
	}
	g.SpanExporter = exporter
	opts = append(opts, sdktrace.WithBatcher(exporter))
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	g.Init = true
	conf.Log.Infof("set tracer success, enableRemote:%v, serviceName:%v, endpoint:%v", enableRemote, serviceName, endpoint)
	return nil
}

func (g *GlobalTracerManager) GetTraceById(traceID string) (root *LocalSpan, err error) {
	g.RLock()
	defer g.RUnlock()
	return g.SpanExporter.GetTraceById(traceID)
}

func (g *GlobalTracerManager) GetTraceByRuleID(ruleID string, limit int64) ([]string, error) {
	g.RLock()
	defer g.RUnlock()
	return g.SpanExporter.GetTraceByRuleID(ruleID, limit)
}

func GetTracer() trace.Tracer {
	globalTracerManager.InitIfNot()
	return otel.GetTracerProvider().Tracer("kuiperd-service")
}

func GetSpanByTraceID(traceID string) (root *LocalSpan, err error) {
	globalTracerManager.InitIfNot()
	return globalTracerManager.GetTraceById(traceID)
}

func SetTracer(config *TracerConfig) error {
	if err := saveTracerConfig(config); err != nil {
		return err
	}
	return globalTracerManager.SetTracer(config.EnableRemoteCollector, config.ServiceName, config.RemoteEndpoint)
}

func InitTracer() error {
	tracerConfig, err := loadTracerConfig()
	if err != nil {
		return err
	}
	return globalTracerManager.SetTracer(tracerConfig.EnableRemoteCollector, tracerConfig.ServiceName, tracerConfig.RemoteEndpoint)
}

func saveTracerConfig(config *TracerConfig) error {
	return conf.SaveCfgKeyToKV(TraceCfgKey, map[string]interface{}{
		"enableRemoteCollector": config.EnableRemoteCollector,
		"serviceName":           config.ServiceName,
		"remoteEndpoint":        config.RemoteEndpoint,
	})
}

func loadTracerConfig() (*TracerConfig, error) {
	tracerConfig := TracerConfigFromConf()
	props, err := conf.LoadCfgKeyKV(TraceCfgKey)
	if err != nil {
		return nil, err
	}
	if err := cast.MapToStruct(props, tracerConfig); err != nil {
		return nil, err
	}
	return tracerConfig, nil
}

func GetTraceIDListByRuleID(ruleID string, limit int64) ([]string, error) {
	globalTracerManager.InitIfNot()
	return globalTracerManager.GetTraceByRuleID(ruleID, limit)
}

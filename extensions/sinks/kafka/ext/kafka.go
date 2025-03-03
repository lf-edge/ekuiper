// Copyright 2023-2023 EMQ Technologies Co., Ltd.
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

package kafka

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/lf-edge/ekuiper/extensions/kafka"
	"github.com/lf-edge/ekuiper/metrics"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

const (
	lblBuild     = "build"
	LblTransform = "transform"
	LblCollect   = "collect"
	LblReq       = "req"
	LblKafka     = "kafka"
	LblMsg       = "msg"
)

type KafkaWriter interface {
	Configure(props map[string]interface{}, m *kafkaSink) error
	Ping(props map[string]interface{}, m *kafkaSink) error
	SetupMeta(ctx api.StreamContext) error
	CollectMessages(ctx api.StreamContext, msgs []map[string]interface{}, m *kafkaSink) error
	SendMessages(ctx api.StreamContext, m *kafkaSink) error
	Close(ctx api.StreamContext) error
}

type kafkaSink struct {
	writer           KafkaWriter
	c                *sinkConf
	kc               *kafkaConf
	tlsConfig        *tls.Config
	sc               kafka.SaslConf
	headersMap       map[string]string
	headerTemplate   string
	LastCollectStats *KafkaCollectStats
}

type sinkConf struct {
	Brokers string `json:"brokers"`
	Topic   string `json:"topic"`
}

type kafkaConf struct {
	kafkaWriterConf
	MaxAttempts  int         `json:"maxAttempts"`
	RequiredACKs int         `json:"requiredACKs"`
	Key          string      `json:"key"`
	Headers      interface{} `json:"headers"`

	// write config
	Compression string `json:"compression"`
}

type kafkaWriterConf struct {
	BatchSize    int           `json:"batchSize"`
	BatchTimeout time.Duration `json:"batchTimeout"`
	BatchBytes   int64         `json:"batchBytes"`
}

func (m *kafkaSink) Ping(_ string, props map[string]interface{}) error {
	return m.writer.Ping(props, m)
}

func (m *kafkaSink) Configure(props map[string]interface{}) error {
	return m.writer.Configure(props, m)
}

func (m *kafkaSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debug("Opening kafka sink")
	m.writer.SetupMeta(ctx)
	return nil
}

func (m *kafkaSink) Collect(ctx api.StreamContext, item interface{}) (err error) {
	defer func() {
		if err == nil {
			KafkaSinkCounter.WithLabelValues(metrics.LblSuccess, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		} else {
			KafkaSinkCounter.WithLabelValues(metrics.LblException, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Inc()
		}
	}()
	m.ResetStats()
	logger := ctx.GetLogger()
	logger.Debugf("kafka sink receive %s", item)
	items := make([]map[string]interface{}, 0, 1)
	start := time.Now()
	switch d := item.(type) {
	case []map[string]interface{}:
		items = d
	case map[string]interface{}:
		items = append(items, d)
	default:
		return fmt.Errorf("unrecognized format of %s", item)
	}
	if err := m.writer.CollectMessages(ctx, items, m); err != nil {
		return err
	}
	cDuration := time.Since(start)
	m.LastCollectStats.TotalCollectMsgDuration += cDuration
	KafkaSinkCollectDurationHist.WithLabelValues(LblCollect, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(cDuration.Microseconds()))
	m.updateMetrics(ctx)
	writeStart := time.Now()
	defer func() {
		metrics.IODurationHist.WithLabelValues(LblKafka, metrics.LblSinkIO, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(time.Since(writeStart).Microseconds()))
	}()
	return m.writer.SendMessages(ctx, m)
}

func (m *kafkaSink) transform(ctx api.StreamContext, msg map[string]any) ([]byte, error) {
	start := time.Now()
	defer func() {
		tDuration := time.Since(start)
		m.LastCollectStats.TotalTransformMsgDuration += tDuration
	}()
	decodedBytes, _, err := ctx.TransformOutput(msg)
	if err != nil {
		return nil, fmt.Errorf("kafka sink transform data error: %v", err)
	}
	return decodedBytes, nil
}

func (m *kafkaSink) Close(ctx api.StreamContext) error {
	return m.writer.Close(ctx)
}

func GetSink() api.Sink {
	return &kafkaSink{}
}

func (m *kafkaSink) setHeaders() error {
	if m.kc.Headers == nil {
		return nil
	}
	switch h := m.kc.Headers.(type) {
	case map[string]interface{}:
		kafkaHeaders := make(map[string]string)
		for key, value := range h {
			if sv, ok := value.(string); ok {
				kafkaHeaders[key] = sv
			}
		}
		m.headersMap = kafkaHeaders
		return nil
	case string:
		m.headerTemplate = h
		return nil
	default:
		return fmt.Errorf("kafka headers must be a map[string]string or a string")
	}
}

func getDefaultKafkaConf() *kafkaConf {
	c := &kafkaConf{
		RequiredACKs: -1,
		MaxAttempts:  1,
	}
	c.kafkaWriterConf = kafkaWriterConf{
		BatchSize: 5000,
		// send batch ASAP
		BatchTimeout: time.Microsecond,
		BatchBytes:   1048576, // 1 MB
	}
	return c
}

func (kc *kafkaConf) configure(props map[string]interface{}) error {
	if err := cast.MapToStruct(props, kc); err != nil {
		return err
	}
	if err := cast.MapToStruct(props, &kc.kafkaWriterConf); err != nil {
		return err
	}
	return nil
}

func toCompression(c string) kafkago.Compression {
	switch strings.ToLower(c) {
	case "gzip":
		return kafkago.Gzip
	case "snappy":
		return kafkago.Snappy
	case "lz4":
		return kafkago.Lz4
	case "zstd":
		return kafkago.Zstd
	}
	return 0
}

// Copyright 2024-2026 EMQ Technologies Co., Ltd.
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

package fvt

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestKafkaConnectionSelectorSinkE2E(t *testing.T) {
	broker := os.Getenv("FVT_KAFKA_BROKER")
	if broker == "" {
		t.Skip("FVT_KAFKA_BROKER is not set")
	}
	require.NoError(t, waitKafkaBroker(broker, 30*time.Second))

	const (
		connID     = "kafkaConnSelectorConn"
		streamName = "kafkaConnSelectorStream"
		ruleID     = "kafkaConnSelectorRule"
		inputTopic = "kafka_conn_selector_input"
	)
	outputTopic := fmt.Sprintf("kafka_conn_selector_output_%d", time.Now().UnixNano())

	cleanupKafkaConnectionSelectorFVT(t)
	t.Cleanup(func() {
		cleanupKafkaConnectionSelectorFVT(t)
	})
	require.NoError(t, createKafkaTopic(broker, outputTopic, 30*time.Second))

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     outputTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1e6,
	})
	require.NoError(t, reader.SetOffset(kafkago.FirstOffset))
	defer reader.Close()

	resp, err := client.Post("connections", fmt.Sprintf(`{
		"id": %q,
		"typ": "kafka",
		"props": {
			"brokers": %q
		}
	}`, connID, broker))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, mustReadResponseText(t, resp))

	resp, err = client.CreateStream(fmt.Sprintf(`{
		"sql": "create stream %s() WITH (TYPE=\"memory\", DATASOURCE=\"%s\", FORMAT=\"json\")"
	}`, streamName, inputTopic))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, mustReadResponseText(t, resp))

	resp, err = client.CreateRule(fmt.Sprintf(`{
		"id": %q,
		"sql": "SELECT * FROM %s",
		"actions": [{
			"kafka": {
				"connectionSelector": %q,
				"topic": %q,
				"batchSize": 1
			}
		}]
	}`, ruleID, streamName, connID, outputTopic))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, mustReadResponseText(t, resp))

	require.True(t, TryAssert(20, ConstantInterval, func() bool {
		get, e := client.Get("connections/" + connID)
		require.NoError(t, e)
		resultMap, e := GetResponseResultMap(get)
		require.NoError(t, e)
		return resultMap["status"] == "connected"
	}), "kafka connection did not reach connected state")
	require.True(t, TryAssert(20, ConstantInterval, func() bool {
		status, e := client.GetRuleStatus(ruleID)
		require.NoError(t, e)
		return status["status"] == "running"
	}), "kafka connection selector rule did not reach running state")

	ctx := mockContext.NewMockContext("kafkaConnectionSelectorFVT", "memoryProducer")
	pubsub.Produce(ctx, inputTopic, &xsql.Tuple{
		Message:   map[string]any{"v": 42, "source": "memory"},
		Metadata:  map[string]any{"topic": inputTopic},
		Timestamp: timex.GetNow(),
	})

	readCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(msg.Value, &got), "kafka payload: %s", string(msg.Value))
	require.Equal(t, float64(42), got["v"])
	require.Equal(t, "memory", got["source"])
}

func cleanupKafkaConnectionSelectorFVT(t *testing.T) {
	t.Helper()
	client.Delete("rules/kafkaConnSelectorRule")
	client.Delete("streams/kafkaConnSelectorStream")
	client.Delete("connections/kafkaConnSelectorConn")
}

func waitKafkaBroker(broker string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", broker, time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return fmt.Errorf("kafka broker %s is not reachable: %w", broker, lastErr)
}

func createKafkaTopic(broker, topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := kafkago.Dial("tcp", broker)
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}
		err = conn.CreateTopics(kafkago.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		_ = conn.Close()
		if err == nil || strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return fmt.Errorf("create kafka topic %s failed: %w", topic, lastErr)
}

func mustReadResponseText(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := GetResponseText(resp)
	require.NoError(t, err)
	return body
}

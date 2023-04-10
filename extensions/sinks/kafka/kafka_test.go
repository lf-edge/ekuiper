// Copyright 2023 carlclone@gmail.com
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

package main

import (
	econf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/transform"
	"testing"
)

func TestKafkaSink(t *testing.T) {
	return
	sink := Kafka()

	props := map[string]interface{}{
		// set any necessary properties
		"brokers":           "127.0.0.1:9092,127.0.0.1:9092",
		"topic":             "test",
		"deliveryGuarantee": "AT_MOST_ONCE",
	}

	contextLogger := econf.Log.WithField("rule", "TestKafkaSink_Apply")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	tf, _ := transform.GenTransform("", "json", "", "")
	vCtx := context.WithValue(ctx, context.TransKey, tf)
	err := sink.Configure(props)
	if err != nil {
		t.Errorf("Error configuring Kafka sink: %s", err)
		return
	}

	err = sink.Open(vCtx)
	if err != nil {
		t.Errorf("Error opening Kafka sink: %s", err)
		return
	}

	var data = []map[string]interface{}{
		{"id": 1, "name": "John", "address": "343", "mobile": "334433"},
		{"id": 2, "name": "Susan", "address": "34", "mobile": "334433"},
		{"id": 3, "name": "Susan", "address": "34", "mobile": "334433"},
	}
	for _, d := range data {
		err = sink.Collect(ctx, d)
		if err != nil {
			t.Error(err)
			return
		}
	}
	sink.Close(ctx)
	if err != nil {
		t.Errorf("Error sending message to Kafka sink: %s", err)
		return
	}

	err = sink.Close(ctx)
	if err != nil {
		t.Errorf("Error closing Kafka sink: %s", err)
		return
	}
}

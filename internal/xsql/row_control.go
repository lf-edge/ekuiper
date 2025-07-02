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

package xsql

import "time"

type WatermarkTuple struct {
	Timestamp time.Time
}

func (t *WatermarkTuple) GetTimestamp() time.Time {
	return t.Timestamp
}

func (t *WatermarkTuple) IsWatermark() bool {
	return true
}

type (
	EOFTuple      string
	BatchEOFTuple time.Time
	// StopTuple indicates rule stop. The content is the rule id. Sends out when Qos is set and rule stops
	StopTuple struct {
		RuleId string
		Sig    int
	}
	StopPrepareTuple struct{}
)

const SigTerm = 1

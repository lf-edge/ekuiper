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

package model

import (
	"io"

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

// Candidate for API. Currently only use internally

type StreamWriter interface {
	CreateWriter(ctx api.StreamContext, currWriter io.Writer, compression string, encryption string) (io.Writer, error)
}

type StreamReader interface {
	CreateWriter(ctx api.StreamContext, currWriter io.Writer, compression string, encryption string) (io.Writer, error)
}

// InfoNode explain the node itself. Mainly used for planner to decide the split of source/sink
type InfoNode interface {
	Info() NodeInfo
}

type NodeInfo struct {
	NeedDecode      bool
	NeedBatchDecode bool // like decrypt, decompress as a whole
}

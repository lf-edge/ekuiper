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

package node

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/converter"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type EncodeOp struct {
	*defaultSinkNode
	converter message.Converter
}

func NewEncodeOp(name string, rOpt *def.RuleOption, sc *SinkConf) (*EncodeOp, error) {
	c, err := converter.GetOrCreateConverter(&ast.Options{FORMAT: sc.Format, SCHEMAID: sc.SchemaId, DELIMITER: sc.Delimiter})
	if err != nil {
		return nil, err
	}
	return &EncodeOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		converter:       c,
	}, nil
}

// Exec decode op receives map/[]map and converts it to bytes.
// If receiving bytes, just return it.
func (o *EncodeOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		err := infra.SafeRun(func() error {
			runWithOrder(ctx, o.defaultSinkNode, o.concurrency, o.Worker)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *EncodeOp) Worker(_ api.Logger, item any) []any {
	o.statManager.ProcessTimeStart()
	defer o.statManager.ProcessTimeEnd()
	switch d := item.(type) {
	case []byte:
		return []any{d}
	default:
		r, err := o.converter.Encode(item)
		if err != nil {
			return []any{err}
		} else {
			return []any{r}
		}
	}
}

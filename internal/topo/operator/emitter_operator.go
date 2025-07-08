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

package operator

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

// EmitterOp set the emitter to the stream name
// It is only planned after shared connection node in which the emitter is not determined in the source side
type EmitterOp struct {
	Emitter string
}

func (p *EmitterOp) Apply(ctx api.StreamContext, data any, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) any {
	ctx.GetLogger().Debugf("emitter op receive %v", data)
	switch input := data.(type) {
	case *xsql.RawTuple:
		input.Emitter = p.Emitter
		return input
	case *xsql.Tuple:
		input.Emitter = p.Emitter
		return input
	case nil:
		return nil
	default:
		return fmt.Errorf("run emitter op error: invalid input %[1]T(%[1]v)", input)
	}
}

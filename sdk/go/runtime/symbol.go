// Copyright 2021 EMQ Technologies Co., Ltd.
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

// Runtime for symbol, to establish data connection

package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/sdk/api"
	"github.com/lf-edge/ekuiper/sdk/connection"
	"github.com/lf-edge/ekuiper/sdk/context"
)

type RuntimeInstance interface {
	run()
	stop() error
	isRunning() bool
}

func broadcast(ctx api.StreamContext, sock connection.DataOutChannel, data interface{}) {
	// encode
	var (
		result []byte
		err    error
	)
	switch dt := data.(type) {
	case error:
		result, err = json.Marshal(fmt.Sprintf("{\"error\":\"%v\"}", dt))
		if err != nil {
			ctx.GetLogger().Errorf("%v", err)
			return
		}
	default:
		result, err = json.Marshal(dt)
		if err != nil {
			ctx.GetLogger().Errorf("%v", err)
			return
		}
	}
	if err = sock.Send(result); err != nil {
		ctx.GetLogger().Errorf("Failed publishing: %s", err.Error())
	}
}

func parseContext(con *Control) (api.StreamContext, error) {
	if con.Meta.RuleId == "" || con.Meta.OpId == "" {
		err := fmt.Sprintf("invalid arg %v, ruleId and opId are required", con)
		context.Log.Errorf(err)
		return nil, fmt.Errorf(err)
	}
	contextLogger := context.LogEntry("rule", con.Meta.RuleId)
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta(con.Meta.RuleId, con.Meta.OpId)
	return ctx, nil
}

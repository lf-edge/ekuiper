// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package infra

import (
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// SafeRun will catch and return the panic error together with other errors
// When running in a rule, the whole rule must be in this mode
// The sub processes or go routines under a rule should also use this mode
// To make sure all rule panic won't affect the whole system
// Also consider running in this mode if the function should not affect the whole system
func SafeRun(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = fmt.Errorf("%#v", x)
			}
		}
	}()
	err = fn()
	return err
}

// DrainError a non-block function to send out the error to the error channel
// Only the first error will be sent out and received then the rule will be terminated
// Thus the latter error will just skip
// It is usually the error outlet of a op/rule.
func DrainError(ctx api.StreamContext, err error, errCh chan<- error) {
	if err != nil {
		if ctx != nil {
			ctx.GetLogger().Errorf("runtime error: %v", err)
		} else {
			conf.Log.Errorf("runtime error: %v", err)
		}
	}
	select {
	case errCh <- err:
	default:
	}
}

// DrainCtrl a non-block function to send out the signal to the ctrl channel
// It will retry in blocking mode once the channel is full and make sure it is delivered finally but may lose order
func DrainCtrl(ctx api.StreamContext, signal any, ctrlCh chan<- any) {
	select {
	case ctrlCh <- signal:
	default:
		ctx.GetLogger().Warnf("failed to send signal %v to ctrl channel, retry", signal)
		go func() {
			ctrlCh <- signal
		}()
	}
}

func SendThrough(ctx api.StreamContext, a api.SourceTuple, consumer chan<- api.SourceTuple) {
	select {
	case consumer <- a:
	default:
		ctx.GetLogger().Warnf("buffer full from %s to decoder, drop message", ctx.GetOpId())
	}
}

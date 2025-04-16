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

package node

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/modules/encryptor"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

// EncryptNode encrypt raw bytes
// Immutable: false
// Input: RawTuple
// Output: RawTuple
type EncryptNode struct {
	*defaultSinkNode
	tool message.Encryptor
}

func NewEncryptOp(name string, rOpt *def.RuleOption, encryptMethod string, encProps map[string]any) (*EncryptNode, error) {
	var key []byte
	switch encryptMethod {
	case "aes":
		if conf.Config == nil || conf.Config.AesKey == nil {
			return nil, fmt.Errorf("AES key is not defined")
		}
		key = conf.Config.AesKey
	}
	dc, err := encryptor.GetEncryptor(encryptMethod, key, encProps)
	if err != nil {
		return nil, fmt.Errorf("get encryptor %s fail with error: %v", encryptMethod, err)
	}
	return &EncryptNode{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		tool:            dc,
	}, nil
}

func (o *EncryptNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer func() {
			o.Close()
		}()
		err := infra.SafeRun(func() error {
			runWithOrder(ctx, o.defaultSinkNode, o.concurrency, o.Worker)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *EncryptNode) Worker(_ api.StreamContext, item any) []any {
	switch d := item.(type) {
	case api.RawTuple:
		r, err := o.tool.Encrypt(d.Raw())
		if err != nil {
			return []any{err}
		}
		d.Replace(r)
		return []any{d}
	default:
		return []any{fmt.Errorf("unsupported data received: %v", d)}
	}
}

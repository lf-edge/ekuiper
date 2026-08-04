// Copyright 2026 EMQ Technologies Co., Ltd.
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

package urlencoded

import (
	"bytes"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type Writer struct {
	converter message.Converter
	buffer    *bytes.Buffer
	isNew     bool
}

func NewWriter(_ api.StreamContext, props map[string]any) (message.ConvertWriter, error) {
	c, err := NewConverter(props)
	if err != nil {
		return nil, err
	}
	return &Writer{
		converter: c,
		buffer:    bytes.NewBuffer(nil),
	}, nil
}

func (w *Writer) New(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("new urlencoded writer")
	w.buffer.Reset()
	w.isNew = true
	return nil
}

func (w *Writer) Write(ctx api.StreamContext, d any) error {
	ctx.GetLogger().Debugf("urlencoded writer write")
	result, err := w.converter.Encode(ctx, d)
	if err != nil {
		return err
	}
	return w.writeRaw(result)
}

func (w *Writer) WriteRaw(ctx api.StreamContext, raw []byte) error {
	ctx.GetLogger().Debugf("urlencoded writer write raw")
	return w.writeRaw(raw)
}

func (w *Writer) writeRaw(raw []byte) error {
	if !w.isNew {
		w.buffer.WriteString("&")
	}
	w.buffer.Write(raw)
	w.isNew = false
	return nil
}

func (w *Writer) Flush(ctx api.StreamContext) ([]byte, error) {
	ctx.GetLogger().Debugf("urlencoded writer flush")
	result := w.buffer.Bytes()
	// Transfer ownership of the completed buffer to the caller.
	w.buffer = bytes.NewBuffer(nil)
	return result, nil
}

var _ message.RawConvertWriter = &Writer{}

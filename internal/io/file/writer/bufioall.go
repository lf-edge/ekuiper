// Copyright 2023 EMQ Technologies Co., Ltd.
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

package writer

import "bufio"

// BufioWrapWriter wraps bufio writer to support flush all once buffer is full
type BufioWrapWriter struct {
	w *bufio.Writer
}

func NewBufioWrapWriter(w *bufio.Writer) *BufioWrapWriter {
	return &BufioWrapWriter{
		w: w,
	}
}

func (b *BufioWrapWriter) Write(p []byte) (int, error) {
	if len(p) > b.w.Available() {
		_ = b.w.Flush()
	}
	return b.w.Write(p)
}

func (b *BufioWrapWriter) Flush() error {
	return b.w.Flush()
}

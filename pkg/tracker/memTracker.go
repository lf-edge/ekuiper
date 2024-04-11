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

package tracker

type Tracker struct {
	par           *Tracker
	bytesConsumed int64
}

func (t *Tracker) Attach(par *Tracker) {
	t.par = par
}

func (t *Tracker) Consume(b int64) {
	if t == nil {
		return
	}
	if t.par != nil {
		t.par.Consume(b)
	}
	t.bytesConsumed += b
}

func (t *Tracker) Release(b int64) {
	if t == nil {
		return
	}
	if t.par != nil {
		t.par.Release(b)
	}
	t.bytesConsumed -= b
	if t.bytesConsumed < 0 {
		t.bytesConsumed = 0
	}
}

func (t *Tracker) BytesConsumed() int64 {
	return t.bytesConsumed
}

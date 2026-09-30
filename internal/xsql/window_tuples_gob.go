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

package xsql

import (
	"bytes"
	"encoding/gob"
)

type windowTuplesDisk struct {
	Content       []Row
	HasRange      bool
	WindowStart   int64
	WindowEnd     int64
	WindowTrigger int64
	CalCols       map[string]any
	AliasMap      map[string]any
	IsAgg         bool
}

// WindowTuples needs explicit encoding because WindowRange has private fields.
func (w *WindowTuples) GobEncode() ([]byte, error) {
	d := windowTuplesDisk{
		Content:  w.Content,
		CalCols:  w.CalCols,
		AliasMap: w.AliasMap,
		IsAgg:    w.isAgg,
	}
	if w.WindowRange != nil {
		d.HasRange = true
		d.WindowStart = w.windowStart
		d.WindowEnd = w.windowEnd
		d.WindowTrigger = w.windowTrigger
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (w *WindowTuples) GobDecode(b []byte) error {
	var d windowTuplesDisk
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&d); err != nil {
		return err
	}
	w.Content = d.Content
	w.ctx = nil
	w.AffiliateRow = AffiliateRow{CalCols: d.CalCols, AliasMap: d.AliasMap}
	w.isAgg = d.IsAgg
	w.WindowRange = nil
	if d.HasRange {
		w.WindowRange = NewWindowRange(d.WindowStart, d.WindowEnd, d.WindowTrigger)
	}
	return nil
}

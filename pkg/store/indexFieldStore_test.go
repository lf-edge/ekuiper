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

package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexFieldStore(t *testing.T) {
	s := &IndexFieldStoreWrap{}
	f := &IndexField{
		IndexFieldName:  "col",
		IndexFieldValue: 1,
	}
	s.Init(f)
	require.Len(t, s.GetFieldList(), 1)
	require.Len(t, s.GetFieldMap(), 1)
	require.Equal(t, s.GetFieldList()[0], s.GetFieldMap()["col"])
	require.Equal(t, s.GetFieldList()[0], s.GetFieldMap()["col"])

	s.UpdateFieldValue("col", 2)
	require.Equal(t, 2, s.GetFieldMap()["col"].IndexFieldValue)
	require.Equal(t, 2, s.GetFieldList()[0].IndexFieldValue)
	s.UpdateFieldValue("col123", 3)

	require.NotNil(t, s.GetStore())
	s = &IndexFieldStoreWrap{}
	s.store = &IndexFieldStore{}
	s.store.IndexFieldValueList = make([]*IndexField, 0)
	s.store.IndexFieldValueList = append(s.store.IndexFieldValueList, &IndexField{
		IndexFieldName:  "col",
		IndexFieldValue: 1,
	})
	s.LoadFromList()
	require.Len(t, s.GetFieldList(), 1)
	require.Len(t, s.GetFieldMap(), 1)
	require.Equal(t, s.GetFieldList()[0], s.GetFieldMap()["col"])

	s.UpdateByInput(map[string]interface{}{
		"col":  2,
		"col3": 4,
	})
	require.Equal(t, 2, s.GetFieldMap()["col"].IndexFieldValue)

	fStore := &IndexFieldStore{}
	s = &IndexFieldStoreWrap{}
	s.InitByStore(fStore)
}

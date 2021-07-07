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

package cast

import (
	"reflect"
	"testing"
)

func TestMapConvert_Funcs(t *testing.T) {
	source := map[interface{}]interface{}{
		"QUERY_TABLE": "VBAP",
		"ROWCOUNT":    10,
		"FIELDS": []interface{}{
			map[interface{}]interface{}{"FIELDNAME": "MANDT"},
			map[interface{}]interface{}{"FIELDNAME": "VBELN"},
			map[interface{}]interface{}{"FIELDNAME": "POSNR"},
		},
	}

	exp := map[string]interface{}{
		"QUERY_TABLE": "VBAP",
		"ROWCOUNT":    10,
		"FIELDS": []interface{}{
			map[string]interface{}{"FIELDNAME": "MANDT"},
			map[string]interface{}{"FIELDNAME": "VBELN"},
			map[string]interface{}{"FIELDNAME": "POSNR"},
		},
	}

	got := ConvertMap(source)
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("result mismatch:\n\nexp=%s\n\ngot=%s\n\n", exp, got)
	}
}

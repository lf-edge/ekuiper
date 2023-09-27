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

package planner

import (
	"reflect"
	"strconv"
	"strings"
)

type ProjectSetPlan struct {
	baseLogicalPlan
	SrfMapping  map[string]struct{}
	enableLimit bool
	limitCount  int
}

func (p ProjectSetPlan) Init() *ProjectSetPlan {
	p.baseLogicalPlan.self = &p
	p.baseLogicalPlan.setPlanType(PROJECTSET)
	return &p
}

func (p *ProjectSetPlan) BuildExplainInfo() {
	info := ""
	if p.SrfMapping != nil && len(p.SrfMapping) != 0 {
		info += "SrfMap:{"
		for str, s := range p.SrfMapping {
			ty := reflect.TypeOf(s)
			arr := strings.Split(ty.String(), ".")
			if len(arr) == 1 {
				info += "key:" + str
			} else {
				info += "key:" + str + ", " + "value:" + arr[1] + ";"
			}
		}
		info += "}"
	}
	info += ", EnableLimit:" + strconv.FormatBool(p.enableLimit)
	p.baseLogicalPlan.ExplainInfo.Info = info
}

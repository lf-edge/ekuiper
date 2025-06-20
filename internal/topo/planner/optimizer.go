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

package planner

import "github.com/lf-edge/ekuiper/v2/internal/pkg/def"

var optRuleList = []logicalOptRule{
	&columnPruner{},
	&predicatePushDown{},
	&pushProjectionPlan{},
	&pushAliasDecode{},
}

func optimize(p LogicalPlan, options *def.RuleOption) (LogicalPlan, error) {
	var err error
	for _, rule := range optRuleList {
		if options.PlanOptimizeStrategy.IsOptimizeEnabled(rule.name()) {
			p, err = rule.optimize(p, options)
			if err != nil {
				return nil, err
			}
		}
	}
	return p, err
}

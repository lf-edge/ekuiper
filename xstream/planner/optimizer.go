package planner

var optRuleList = []logicalOptRule{
	&predicatePushDown{},
}

func optimize(p LogicalPlan) (LogicalPlan, error) {
	var err error
	for _, rule := range optRuleList {
		p, err = rule.optimize(p)
		if err != nil {
			return nil, err
		}
	}
	return p, err
}

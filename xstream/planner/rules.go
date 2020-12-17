package planner

type logicalOptRule interface {
	optimize(LogicalPlan) (LogicalPlan, error)
	name() string
}

type predicatePushDown struct{}

func (r *predicatePushDown) optimize(lp LogicalPlan) (LogicalPlan, error) {
	_, p := lp.PushDownPredicate(nil)
	return p, nil
}

func (r *predicatePushDown) name() string {
	return "predicatePushDown"
}

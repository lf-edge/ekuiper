package planner

import (
	"bytes"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type PhysicalPlan interface {
	GetIndex() int
	GetChildren() []PhysicalPlan
	AddChild(PhysicalPlan)
	SetChildren([]PhysicalPlan)
	ExplainInfo() string
}

type BasePhysicalPlan struct {
	Type     string
	Index    int
	Children []PhysicalPlan
}

func NewBasePhysicalPlan(index int, Type string) *BasePhysicalPlan {
	return &BasePhysicalPlan{Type: Type, Index: index, Children: make([]PhysicalPlan, 0)}
}

func (b *BasePhysicalPlan) ExplainInfo() string {
	return fmt.Sprintf("%v_%v", b.Type, b.Index)
}

func (b *BasePhysicalPlan) AddChild(plan PhysicalPlan) {
	b.Children = append(b.Children, plan)
}

func (b *BasePhysicalPlan) SetChildren(plans []PhysicalPlan) {
	b.Children = plans
}

func (b *BasePhysicalPlan) GetIndex() int {
	return b.Index
}

func (b *BasePhysicalPlan) GetChildren() []PhysicalPlan {
	return b.Children
}

type PhysicalDataSource struct {
	Stream *catalog.Stream
	*BasePhysicalPlan
}

func NewPhysicalDataSource(ds *DataSourcePlan, index int) *PhysicalDataSource {
	pd := &PhysicalDataSource{
		Stream:           ds.Stream,
		BasePhysicalPlan: NewBasePhysicalPlan(index, "Datasource"),
	}
	return pd
}

type PhysicalDataSink struct {
	SinkType  string
	SinkProps map[string]interface{}
	*BasePhysicalPlan
}

func NewPhysicalDataSink(ds *DataSinkPlan, index int) *PhysicalDataSink {
	pd := &PhysicalDataSink{
		SinkType:         ds.SinkType,
		SinkProps:        make(map[string]interface{}),
		BasePhysicalPlan: NewBasePhysicalPlan(index, "DataSink"),
	}
	return pd
}

type PhysicalProject struct {
	Fields ast.Fields
	*BasePhysicalPlan
}

func NewPhysicalProject(proj *ProjectPlan, index int) *PhysicalProject {
	return &PhysicalProject{
		Fields:           proj.Fields,
		BasePhysicalPlan: NewBasePhysicalPlan(index, "Project"),
	}
}

type PhysicalStake struct {
	IsRoot bool
	IsEnd  bool
	*BasePhysicalPlan
}

func NewPhysicalStakeRoot(root *LogicalPlanRoot, index int) *PhysicalStake {
	return &PhysicalStake{
		IsRoot:           true,
		BasePhysicalPlan: NewBasePhysicalPlan(index, "Stake"),
	}
}

func NewPhysicalStakeEnd(root *LogicalPlanEnd, index int) *PhysicalStake {
	return &PhysicalStake{
		IsEnd:            true,
		BasePhysicalPlan: NewBasePhysicalPlan(index, "Stake"),
	}
}

func ExplainPhysicalPlan(lp PhysicalPlan) string {
	buf := bytes.NewBufferString("")
	explainPhysicalPlan(lp, 0, buf)
	return buf.String()
}

func explainPhysicalPlan(lp PhysicalPlan, level int, buffer *bytes.Buffer) {
	for i := 0; i < level; i++ {
		buffer.WriteString("  ")
	}
	buffer.WriteString(lp.ExplainInfo())
	buffer.WriteString("\n")
	for _, child := range lp.GetChildren() {
		explainPhysicalPlan(child, level+1, buffer)
	}
}

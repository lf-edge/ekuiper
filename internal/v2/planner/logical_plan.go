package planner

import (
	"bytes"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type LogicalPlan interface {
	GetIndex() int
	GetChildren() []LogicalPlan
	AddChild(LogicalPlan)
	SetChildren([]LogicalPlan)
	ExplainInfo() string
}

type BaseLogicalPlan struct {
	Type     string
	Index    int
	Children []LogicalPlan
}

func (b *BaseLogicalPlan) ExplainInfo() string {
	return fmt.Sprintf("%s_%d", b.Type, b.Index)
}

func (b *BaseLogicalPlan) SetChildren(plans []LogicalPlan) {
	b.Children = plans
}

func (b *BaseLogicalPlan) AddChild(plan LogicalPlan) {
	b.Children = append(b.Children, plan)
}

func (b *BaseLogicalPlan) GetIndex() int {
	return b.Index
}

func (b *BaseLogicalPlan) GetChildren() []LogicalPlan {
	return b.Children
}

func NewBaseLogicalPlan(Index int, Type string) *BaseLogicalPlan {
	return &BaseLogicalPlan{Type: Type, Index: Index, Children: make([]LogicalPlan, 0)}
}

type LogicalPlanRoot struct {
	*BaseLogicalPlan
}

func NewLogicalPlanRoot(index int) *LogicalPlanRoot {
	return &LogicalPlanRoot{BaseLogicalPlan: NewBaseLogicalPlan(index, "root")}
}

type LogicalPlanEnd struct {
	*BaseLogicalPlan
}

func NewLogicalPlanEnd(index int) *LogicalPlanEnd {
	return &LogicalPlanEnd{BaseLogicalPlan: NewBaseLogicalPlan(index, "end")}
}

type DataSourcePlan struct {
	Stream *catalog.Stream
	*BaseLogicalPlan
}

func NewDataSourcePlan(stream *catalog.Stream, index int) *DataSourcePlan {
	return &DataSourcePlan{Stream: stream, BaseLogicalPlan: NewBaseLogicalPlan(index, "DataSource")}
}

type ProjectPlan struct {
	Fields ast.Fields
	*BaseLogicalPlan
}

func NewProjectPlan(index int, fields ast.Fields) *ProjectPlan {
	return &ProjectPlan{Fields: fields, BaseLogicalPlan: NewBaseLogicalPlan(index, "Project")}
}

func ExplainLogicalPlan(lp LogicalPlan) string {
	buf := bytes.NewBufferString("")
	explainLogicalPlan(lp, 0, buf)
	return buf.String()
}

func explainLogicalPlan(lp LogicalPlan, level int, buffer *bytes.Buffer) {
	for i := 0; i < level; i++ {
		buffer.WriteString("  ")
	}
	buffer.WriteString(lp.ExplainInfo())
	buffer.WriteString("\n")
	for _, child := range lp.GetChildren() {
		explainLogicalPlan(child, level+1, buffer)
	}
}

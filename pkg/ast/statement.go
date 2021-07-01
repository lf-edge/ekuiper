package ast

type Statement interface {
	stmt()
	Node
}

type SelectStatement struct {
	Fields     Fields
	Sources    Sources
	Joins      Joins
	Condition  Expr
	Dimensions Dimensions
	Having     Expr
	SortFields SortFields

	Statement
}

type Fields []Field

func (f Fields) node() {}

func (f Fields) Len() int {
	return len(f)
}
func (f Fields) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}
func (f Fields) Less(i int, j int) bool {
	m := f[i].AName
	if m == "" {
		m = f[i].Name
	}
	n := f[j].AName
	if n == "" {
		n = f[j].Name
	}
	return m < n
}

type Field struct {
	Name  string
	AName string
	Expr  Expr

	Node
}

func (f *Field) GetName() string {
	if f.AName != "" {
		return f.AName
	} else {
		return f.Name
	}
}

func (f *Field) IsSelectionField() bool {
	if f.AName != "" {
		return true
	}
	_, ok := f.Expr.(*FieldRef)
	if ok {
		return true
	}
	return false
}

func (f *Field) IsColumn() bool {
	if f.AName != "" {
		return false
	}
	_, ok := f.Expr.(*FieldRef)
	if ok {
		return true
	}
	return false
}

type Sources []Source

func (s Sources) node() {}

type Source interface {
	Node
	source()
}

type Table struct {
	Name  string
	Alias string
	Source
}

type JoinType int

const (
	LEFT_JOIN JoinType = iota
	INNER_JOIN
	RIGHT_JOIN
	FULL_JOIN
	CROSS_JOIN
)

type Join struct {
	Name     string
	Alias    string
	JoinType JoinType
	Expr     Expr

	Node
}

type Joins []Join

func (j Joins) node() {}

type Dimension struct {
	Expr Expr

	Node
}

type Dimensions []Dimension

func (d Dimensions) node() {}

func (d *Dimensions) GetWindow() *Window {
	for _, child := range *d {
		if w, ok := child.Expr.(*Window); ok {
			return w
		}
	}
	return nil
}
func (d *Dimensions) GetGroups() Dimensions {
	var nd Dimensions
	for _, child := range *d {
		if _, ok := child.Expr.(*Window); !ok {
			nd = append(nd, child)
		}
	}
	return nd
}

type WindowType int

const (
	NOT_WINDOW WindowType = iota
	TUMBLING_WINDOW
	HOPPING_WINDOW
	SLIDING_WINDOW
	SESSION_WINDOW
	COUNT_WINDOW
)

type Window struct {
	WindowType WindowType
	Length     *IntegerLiteral
	Interval   *IntegerLiteral
	Filter     Expr
	Expr
}

type SortField struct {
	Name      string
	Ascending bool

	Expr
}

type SortFields []SortField

func (d SortFields) node() {}

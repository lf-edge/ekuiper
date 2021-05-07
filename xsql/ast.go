package xsql

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
)

type Node interface {
	node()
}

type NameNode interface {
	Node
	GetName() string
}

type Expr interface {
	Node
	expr()
}

type Field struct {
	Name  string
	AName string
	Expr
}

type Source interface {
	Node
	source()
}

type Sources []Source

func (ss Sources) node() {}

type Table struct {
	Name  string
	Alias string
}

func (t *Table) source() {}
func (ss *Table) node()  {}

type JoinType int

const (
	LEFT_JOIN JoinType = iota
	INNER_JOIN
	RIGHT_JOIN
	FULL_JOIN
	CROSS_JOIN
)

var AsteriskExpr = StringLiteral{Val: "*"}

var COLUMN_SEPARATOR = tokens[COLSEP]

type Join struct {
	Name     string
	Alias    string
	JoinType JoinType
	Expr     Expr
}

func (j *Join) source() {}
func (ss *Join) node()  {}

type Joins []Join

func (ss Joins) node() {}

type Statement interface {
	Stmt()
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
}

func (ss *SelectStatement) Stmt() {}
func (ss *SelectStatement) node() {}

type Literal interface {
	Expr
	literal()
}

type ParenExpr struct {
	Expr Expr
}

type ArrowExpr struct {
	Expr Expr
}

type BracketExpr struct {
	Expr Expr
}

type ColonExpr struct {
	Start int
	End   int
}

type IndexExpr struct {
	Index int
}

type BooleanLiteral struct {
	Val bool
}

type TimeLiteral struct {
	Val Token
}

type IntegerLiteral struct {
	Val int
}

type StringLiteral struct {
	Val string
}

type NumberLiteral struct {
	Val float64
}

type Wildcard struct {
	Token Token
}

type Dimension struct {
	Expr Expr
}

type SortField struct {
	Name      string
	Ascending bool
}

type SortFields []SortField

type Dimensions []Dimension

func (f *Field) expr() {}
func (f *Field) node() {}

func (pe *ParenExpr) expr() {}
func (pe *ParenExpr) node() {}

func (ae *ArrowExpr) expr() {}
func (ae *ArrowExpr) node() {}

func (be *BracketExpr) expr() {}
func (be *BracketExpr) node() {}

func (be *ColonExpr) expr() {}
func (be *ColonExpr) node() {}

func (be *IndexExpr) expr() {}
func (be *IndexExpr) node() {}

func (w *Wildcard) expr() {}
func (w *Wildcard) node() {}

func (bl *BooleanLiteral) expr()    {}
func (bl *BooleanLiteral) literal() {}
func (bl *BooleanLiteral) node()    {}

func (tl *TimeLiteral) expr()    {}
func (tl *TimeLiteral) literal() {}
func (tl *TimeLiteral) node()    {}

func (il *IntegerLiteral) expr()    {}
func (il *IntegerLiteral) literal() {}
func (il *IntegerLiteral) node()    {}

func (nl *NumberLiteral) expr()    {}
func (nl *NumberLiteral) literal() {}
func (nl *NumberLiteral) node()    {}

func (sl *StringLiteral) expr()    {}
func (sl *StringLiteral) literal() {}
func (sl *StringLiteral) node()    {}

func (d *Dimension) expr() {}
func (d *Dimension) node() {}

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

func (sf *SortField) expr() {}
func (sf *SortField) node() {}

func (sf SortFields) node() {}

type Call struct {
	Name string
	Args []Expr
}

func (c *Call) expr()    {}
func (c *Call) literal() {}
func (c *Call) node()    {}

type WhenClause struct {
	// The condition expression
	Expr   Expr
	Result Expr
}

func (w *WhenClause) expr()    {}
func (w *WhenClause) literal() {}
func (w *WhenClause) node()    {}

type CaseExpr struct {
	// The compare value expression. It can be a value expression or nil.
	// When it is nil, the WhenClause Expr must be a logical(comparison) expression
	Value       Expr
	WhenClauses []*WhenClause
	ElseClause  Expr
}

func (c *CaseExpr) expr()    {}
func (c *CaseExpr) literal() {}
func (c *CaseExpr) node()    {}

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
}

func (w *Window) expr()    {}
func (w *Window) literal() {}
func (w *Window) node()    {}

type SelectStatements []SelectStatement

func (ss *SelectStatements) node() {}

type Fields []Field

func (fs Fields) node() {}

type BinaryExpr struct {
	OP  Token
	LHS Expr
	RHS Expr
}

func (fe *BinaryExpr) expr() {}
func (be *BinaryExpr) node() {}

type FieldRef struct {
	StreamName StreamName
	Name       string
}

func (fr *FieldRef) expr() {}
func (fr *FieldRef) node() {}

type MetaRef struct {
	StreamName StreamName
	Name       string
}

func (fr *MetaRef) expr() {}
func (fr *MetaRef) node() {}

// The stream AST tree
type Options struct {
	DATASOURCE        string
	KEY               string
	FORMAT            string
	CONF_KEY          string
	TYPE              string
	STRICT_VALIDATION bool
	TIMESTAMP         string
	TIMESTAMP_FORMAT  string
	RETAIN_SIZE       int
}

func (o Options) node() {}

type StreamName string

func (sn *StreamName) node() {}

type StreamType int

const (
	TypeStream StreamType = iota
	TypeTable
)

var StreamTypeMap = map[StreamType]string{
	TypeStream: "stream",
	TypeTable:  "table",
}

type StreamStmt struct {
	Name         StreamName
	StreamFields StreamFields
	Options      *Options
	StreamType   StreamType //default to TypeStream
}

func (ss *StreamStmt) node() {}
func (ss *StreamStmt) Stmt() {}

type FieldType interface {
	fieldType()
	Node
}

type StreamField struct {
	Name string
	FieldType
}

func (u *StreamField) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		FieldType interface{}
		Name      string
	}{
		FieldType: PrintFieldTypeForJson(u.FieldType),
		Name:      u.Name,
	})
}

type StreamFields []StreamField

func (sf StreamFields) node() {}

type BasicType struct {
	Type DataType
}

func (bt *BasicType) fieldType() {}
func (bt *BasicType) node()      {}

type ArrayType struct {
	Type DataType
	FieldType
}

func (at *ArrayType) fieldType() {}
func (at *ArrayType) node()      {}

type RecType struct {
	StreamFields StreamFields
}

func (rt *RecType) fieldType() {}
func (rt *RecType) node()      {}

type ShowStreamsStatement struct {
}

type DescribeStreamStatement struct {
	Name string
}

type ExplainStreamStatement struct {
	Name string
}

type DropStreamStatement struct {
	Name string
}

func (ss *ShowStreamsStatement) Stmt() {}
func (ss *ShowStreamsStatement) node() {}

func (dss *DescribeStreamStatement) Stmt()           {}
func (dss *DescribeStreamStatement) node()           {}
func (dss *DescribeStreamStatement) GetName() string { return dss.Name }

func (ess *ExplainStreamStatement) Stmt()           {}
func (ess *ExplainStreamStatement) node()           {}
func (ess *ExplainStreamStatement) GetName() string { return ess.Name }

func (dss *DropStreamStatement) Stmt()           {}
func (dss *DropStreamStatement) node()           {}
func (dss *DropStreamStatement) GetName() string { return dss.Name }

type ShowTablesStatement struct {
}

type DescribeTableStatement struct {
	Name string
}

type ExplainTableStatement struct {
	Name string
}

type DropTableStatement struct {
	Name string
}

func (ss *ShowTablesStatement) Stmt() {}
func (ss *ShowTablesStatement) node() {}

func (dss *DescribeTableStatement) Stmt()           {}
func (dss *DescribeTableStatement) node()           {}
func (dss *DescribeTableStatement) GetName() string { return dss.Name }

func (ess *ExplainTableStatement) Stmt()           {}
func (ess *ExplainTableStatement) node()           {}
func (ess *ExplainTableStatement) GetName() string { return ess.Name }

func (dss *DropTableStatement) Stmt()           {}
func (dss *DropTableStatement) node()           {}
func (dss *DropTableStatement) GetName() string { return dss.Name }

type Visitor interface {
	Visit(Node) Visitor
}

func Walk(v Visitor, node Node) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {

	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *Call:
		for _, expr := range n.Args {
			Walk(v, expr)
		}

	case *Window:
		Walk(v, n.Length)
		Walk(v, n.Interval)

	case *Field:
		Walk(v, n.Expr)

	case Fields:
		for _, c := range n {
			Walk(v, &c)
		}

	case *ParenExpr:
		Walk(v, n.Expr)

	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Dimensions)
		Walk(v, n.Sources)
		Walk(v, n.Joins)
		Walk(v, n.Condition)
		Walk(v, n.SortFields)
		Walk(v, n.Having)

	case SortFields:
		for _, sf := range n {
			Walk(v, &sf)
		}

	case Sources:
		for _, s := range n {
			Walk(v, s)
		}

	case Joins:
		for _, s := range n {
			Walk(v, &s)
		}
	case *Join:
		Walk(v, n.Expr)

	case *CaseExpr:
		Walk(v, n.Value)
		for _, w := range n.WhenClauses {
			Walk(v, w)
		}
		Walk(v, n.ElseClause)

	case *WhenClause:
		Walk(v, n.Expr)
		Walk(v, n.Result)

	case *StreamStmt:
		Walk(v, &n.Name)
		Walk(v, n.StreamFields)
		Walk(v, n.Options)

	case *BasicType, *ArrayType, *RecType:
		Walk(v, n)

	case *ShowStreamsStatement, *DescribeStreamStatement, *ExplainStreamStatement, *DropStreamStatement,
		*ShowTablesStatement, *DescribeTableStatement, *ExplainTableStatement, *DropTableStatement:
		Walk(v, n)
	}
}

// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node)

func (fn walkFuncVisitor) Visit(n Node) Visitor { fn(n); return fn }

// Valuer is the interface that wraps the Value() method.
type Valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key string) (interface{}, bool)
	Meta(key string) (interface{}, bool)
}

// CallValuer implements the Call method for evaluating function calls.
type CallValuer interface {
	Valuer

	// Call is invoked to evaluate a function call (if possible).
	Call(name string, args []interface{}) (interface{}, bool)
}

type AggregateCallValuer interface {
	CallValuer
	GetAllTuples() AggregateData
	GetSingleCallValuer() CallValuer
}

type Wildcarder interface {
	// Value returns the value and existence flag for a given key.
	All(stream string) (interface{}, bool)
}

type DataValuer interface {
	Valuer
	Wildcarder
}

type WildcardValuer struct {
	Data Wildcarder
}

//TODO deal with wildcard of a stream, e.g. SELECT Table.* from Table inner join Table1
func (wv *WildcardValuer) Value(key string) (interface{}, bool) {
	if key == "" {
		return wv.Data.All(key)
	} else {
		a := strings.Index(key, COLUMN_SEPARATOR+"*")
		if a <= 0 {
			return nil, false
		} else {
			return wv.Data.All(key[:a])
		}
	}
}

func (wv *WildcardValuer) Meta(key string) (interface{}, bool) {
	return nil, false
}

/**********************************
**	Various Data Types for SQL transformation
 */

type AggregateData interface {
	AggregateEval(expr Expr, v CallValuer) []interface{}
}

// Message is a valuer that substitutes values for the mapped interface.
type Message map[string]interface{}

func ToMessage(input interface{}) (Message, bool) {
	var result Message
	switch m := input.(type) {
	case Message:
		result = m
	case Metadata:
		result = Message(m)
	case map[string]interface{}:
		result = m
	default:
		return nil, false
	}
	return result, true
}

// Value returns the value for a key in the Message.
func (m Message) Value(key string) (interface{}, bool) {
	var colkey string
	if keys := strings.Split(key, COLUMN_SEPARATOR); len(keys) == 1 {
		colkey = key
	} else if len(keys) == 2 {
		colkey = keys[1]
	} else {
		common.Log.Println("Invalid key: " + key + ", expect source.field or field.")
		return nil, false
	}
	key1 := strings.ToLower(colkey)
	if v, ok := m[key1]; ok {
		return v, ok
	} else {
		//Only when with 'SELECT * FROM ...'  and 'schemaless', the key in map is not convert to lower case.
		//So all of keys in map should be convert to lowercase and then compare them.
		return m.getIgnoreCase(colkey)
	}
}

func (m Message) getIgnoreCase(key interface{}) (interface{}, bool) {
	if k, ok := key.(string); ok {
		key = strings.ToLower(k)
		for k, v := range m {
			if strings.ToLower(k) == key {
				return v, true
			}
		}
	}
	return nil, false
}

func (m Message) Meta(key string) (interface{}, bool) {
	if key == "*" {
		return map[string]interface{}(m), true
	}
	return m.Value(key)
}

type Event interface {
	GetTimestamp() int64
	IsWatermark() bool
}

type Metadata Message

func (m Metadata) Value(key string) (interface{}, bool) {
	msg := Message(m)
	return msg.Value(key)
}

func (m Metadata) Meta(key string) (interface{}, bool) {
	if key == "*" {
		return map[string]interface{}(m), true
	}
	msg := Message(m)
	return msg.Meta(key)
}

type Tuple struct {
	Emitter   string
	Message   Message
	Timestamp int64
	Metadata  Metadata
}

func (t *Tuple) Value(key string) (interface{}, bool) {
	return t.Message.Value(key)
}

func (t *Tuple) Meta(key string) (interface{}, bool) {
	if key == "*" {
		return map[string]interface{}(t.Metadata), true
	}
	return t.Metadata.Value(key)
}

func (t *Tuple) All(stream string) (interface{}, bool) {
	return t.Message, true
}

func (t *Tuple) AggregateEval(expr Expr, v CallValuer) []interface{} {
	return []interface{}{Eval(expr, MultiValuer(t, v, &WildcardValuer{t}))}
}

func (t *Tuple) GetTimestamp() int64 {
	return t.Timestamp
}

func (t *Tuple) GetMetadata() Metadata {
	return t.Metadata
}

func (t *Tuple) IsWatermark() bool {
	return false
}

type WindowTuples struct {
	Emitter string
	Tuples  []Tuple
}

type WindowTuplesSet []WindowTuples

func (w WindowTuplesSet) GetBySrc(src string) []Tuple {
	for _, me := range w {
		if me.Emitter == src {
			return me.Tuples
		}
	}
	return nil
}

func (w WindowTuplesSet) Len() int {
	if len(w) > 0 {
		return len(w[0].Tuples)
	}
	return 0
}
func (w WindowTuplesSet) Swap(i, j int) {
	if len(w) > 0 {
		s := w[0].Tuples
		s[i], s[j] = s[j], s[i]
	}
}
func (w WindowTuplesSet) Index(i int) Valuer {
	if len(w) > 0 {
		s := w[0].Tuples
		return &(s[i])
	}
	return nil
}

func (w WindowTuplesSet) AddTuple(tuple *Tuple) WindowTuplesSet {
	found := false
	for i, t := range w {
		if t.Emitter == tuple.Emitter {
			t.Tuples = append(t.Tuples, *tuple)
			found = true
			w[i] = t
			break
		}
	}

	if !found {
		ets := &WindowTuples{Emitter: tuple.Emitter}
		ets.Tuples = append(ets.Tuples, *tuple)
		w = append(w, *ets)
	}
	return w
}

//Sort by tuple timestamp
func (w WindowTuplesSet) Sort() {
	for _, t := range w {
		tuples := t.Tuples
		sort.SliceStable(tuples, func(i, j int) bool {
			return tuples[i].Timestamp < tuples[j].Timestamp
		})
		t.Tuples = tuples
	}
}

func (w WindowTuplesSet) AggregateEval(expr Expr, v CallValuer) []interface{} {
	var result []interface{}
	if len(w) != 1 { //should never happen
		return nil
	}
	for _, t := range w[0].Tuples {
		result = append(result, Eval(expr, MultiValuer(&t, v, &WildcardValuer{&t})))
	}
	return result
}

type JoinTuple struct {
	Tuples []Tuple
}

func (jt *JoinTuple) AddTuple(tuple Tuple) {
	jt.Tuples = append(jt.Tuples, tuple)
}

func (jt *JoinTuple) AddTuples(tuples []Tuple) {
	for _, t := range tuples {
		jt.Tuples = append(jt.Tuples, t)
	}
}

func getTupleValue(tuple Tuple, t string, key string) (interface{}, bool) {
	switch t {
	case "value":
		return tuple.Value(key)
	case "meta":
		return tuple.Meta(key)
	default:
		common.Log.Errorf("cannot get tuple for type %s", t)
		return nil, false
	}
}

func (jt *JoinTuple) doGetValue(t string, key string) (interface{}, bool) {
	keys := strings.Split(key, COLUMN_SEPARATOR)
	tuples := jt.Tuples
	switch len(keys) {
	case 1:
		if len(tuples) > 1 {
			for _, tuple := range tuples { //TODO support key without modifier?
				v, ok := getTupleValue(tuple, t, key)
				if ok {
					return v, ok
				}
			}
			common.Log.Debugf("Wrong key: %s not found", key)
			return nil, false
		} else {
			return getTupleValue(tuples[0], t, key)
		}
	case 2:
		emitter, key := keys[0], keys[1]
		//TODO should use hash here
		for _, tuple := range tuples {
			if tuple.Emitter == emitter {
				return getTupleValue(tuple, t, key)
			}
		}
		return nil, false
	default:
		common.Log.Infoln("Wrong key: ", key, ", expect dot in the expression.")
		return nil, false
	}
}

func (jt *JoinTuple) Value(key string) (interface{}, bool) {
	return jt.doGetValue("value", key)
}

func (jt *JoinTuple) Meta(key string) (interface{}, bool) {
	return jt.doGetValue("meta", key)
}

func (jt *JoinTuple) All(stream string) (interface{}, bool) {
	if stream != "" {
		for _, t := range jt.Tuples {
			if t.Emitter == stream {
				return t.Message, true
			}
		}
	} else {
		var r Message = make(map[string]interface{})
		for _, t := range jt.Tuples {
			for k, v := range t.Message {
				if _, ok := r[k]; !ok {
					r[k] = v
				}
			}
		}
		return r, true
	}
	return nil, false
}

type JoinTupleSets []JoinTuple

func (s JoinTupleSets) Len() int           { return len(s) }
func (s JoinTupleSets) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s JoinTupleSets) Index(i int) Valuer { return &(s[i]) }

func (s JoinTupleSets) AggregateEval(expr Expr, v CallValuer) []interface{} {
	var result []interface{}
	for _, t := range s {
		result = append(result, Eval(expr, MultiValuer(&t, v, &WildcardValuer{&t})))
	}
	return result
}

type GroupedTuples []DataValuer

func (s GroupedTuples) AggregateEval(expr Expr, v CallValuer) []interface{} {
	var result []interface{}
	for _, t := range s {
		result = append(result, Eval(expr, MultiValuer(t, v, &WildcardValuer{t})))
	}
	return result
}

type GroupedTuplesSet []GroupedTuples

func (s GroupedTuplesSet) Len() int           { return len(s) }
func (s GroupedTuplesSet) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s GroupedTuplesSet) Index(i int) Valuer { return s[i][0] }

type SortingData interface {
	Len() int
	Swap(i, j int)
	Index(i int) Valuer
}

// multiSorter implements the Sort interface, sorting the changes within.Hi
type MultiSorter struct {
	SortingData
	fields SortFields
	valuer CallValuer
	values []map[string]interface{}
}

// OrderedBy returns a Sorter that sorts using the less functions, in order.
// Call its Sort method to sort the data.
func OrderedBy(fields SortFields, fv *FunctionValuer) *MultiSorter {
	return &MultiSorter{
		fields: fields,
		valuer: fv,
	}
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call. We could change the functions to return
// -1, 0, 1 and reduce the number of calls for greater efficiency: an
// exercise for the reader.
func (ms *MultiSorter) Less(i, j int) bool {
	p, q := ms.values[i], ms.values[j]
	v := &ValuerEval{Valuer: MultiValuer(ms.valuer)}
	for _, field := range ms.fields {
		n := field.Name
		vp, _ := p[n]
		vq, _ := q[n]
		if vp == nil && vq != nil {
			return false
		} else if vp != nil && vq == nil {
			ms.valueSwap(true, i, j)
			return true
		} else if vp == nil && vq == nil {
			return false
		}
		switch {
		case v.simpleDataEval(vp, vq, LT):
			ms.valueSwap(field.Ascending, i, j)
			return field.Ascending
		case v.simpleDataEval(vq, vp, LT):
			ms.valueSwap(!field.Ascending, i, j)
			return !field.Ascending
		}
	}
	return false
}

func (ms *MultiSorter) valueSwap(s bool, i, j int) {
	if s {
		ms.values[i], ms.values[j] = ms.values[j], ms.values[i]
	}
}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (ms *MultiSorter) Sort(data SortingData) error {
	ms.SortingData = data
	types := make([]string, len(ms.fields))
	ms.values = make([]map[string]interface{}, data.Len())
	//load and validate data
	for i := 0; i < data.Len(); i++ {
		ms.values[i] = make(map[string]interface{})
		p := data.Index(i)
		vep := &ValuerEval{Valuer: MultiValuer(p, ms.valuer)}
		for j, field := range ms.fields {
			n := field.Name
			vp, _ := vep.Valuer.Value(n)
			if err, ok := vp.(error); ok {
				return err
			} else {
				if types[j] == "" && vp != nil {
					types[j] = fmt.Sprintf("%T", vp)
				}
				if err := validate(types[j], vp); err != nil {
					return err
				} else {
					ms.values[i][n] = vp
				}
			}
		}
	}
	sort.Sort(ms)
	return nil
}

func validate(t string, v interface{}) error {
	if v == nil || t == "" {
		return nil
	}
	vt := fmt.Sprintf("%T", v)
	switch t {
	case "int", "int64", "float64", "uint64":
		if vt == "int" || vt == "int64" || vt == "float64" || vt == "uint64" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "bool":
		if vt == "bool" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "string":
		if vt == "string" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "time.Time":
		_, err := common.InterfaceToTime(v, "")
		if err != nil {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		} else {
			return nil
		}
	default:
		return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
	}
	return nil
}

type EvalResultMessage struct {
	Emitter string
	Result  interface{}
	Message Message
}

type ResultsAndMessages []EvalResultMessage

// Eval evaluates expr against a map.
func Eval(expr Expr, m Valuer) interface{} {
	eval := ValuerEval{Valuer: m}
	return eval.Eval(expr)
}

// ValuerEval will evaluate an expression using the Valuer.
type ValuerEval struct {
	Valuer Valuer

	// IntegerFloatDivision will set the eval system to treat
	// a division between two integers as a floating point division.
	IntegerFloatDivision bool
}

// MultiValuer returns a Valuer that iterates over multiple Valuer instances
// to find a match.
func MultiValuer(valuers ...Valuer) Valuer {
	return multiValuer(valuers)
}

type multiValuer []Valuer

func (a multiValuer) Value(key string) (interface{}, bool) {
	for _, valuer := range a {
		if v, ok := valuer.Value(key); ok {
			return v, true
		}
	}
	return nil, false
}

func (a multiValuer) Meta(key string) (interface{}, bool) {
	for _, valuer := range a {
		if v, ok := valuer.Meta(key); ok {
			return v, true
		}
	}
	return nil, false
}

func (a multiValuer) Call(name string, args []interface{}) (interface{}, bool) {
	for _, valuer := range a {
		if valuer, ok := valuer.(CallValuer); ok {
			if v, ok := valuer.Call(name, args); ok {
				return v, true
			} else {
				return fmt.Errorf("call func %s error: %v", name, v), false
			}
		}
	}
	return nil, false
}

type multiAggregateValuer struct {
	data AggregateData
	multiValuer
	singleCallValuer CallValuer
}

func MultiAggregateValuer(data AggregateData, singleCallValuer CallValuer, valuers ...Valuer) Valuer {
	return &multiAggregateValuer{
		data:             data,
		multiValuer:      valuers,
		singleCallValuer: singleCallValuer,
	}
}

func (a *multiAggregateValuer) Call(name string, args []interface{}) (interface{}, bool) {
	// assume the aggFuncMap already cache the custom agg funcs in isAggFunc()
	_, isAgg := aggFuncMap[name]
	for _, valuer := range a.multiValuer {
		if a, ok := valuer.(AggregateCallValuer); ok && isAgg {
			if v, ok := a.Call(name, args); ok {
				return v, true
			} else {
				return fmt.Errorf("call func %s error: %v", name, v), false
			}
		} else if c, ok := valuer.(CallValuer); ok && !isAgg {
			if v, ok := c.Call(name, args); ok {
				return v, true
			}
		}
	}
	return nil, false
}

func (a *multiAggregateValuer) GetAllTuples() AggregateData {
	return a.data
}

func (a *multiAggregateValuer) GetSingleCallValuer() CallValuer {
	return a.singleCallValuer
}

type BracketEvalResult struct {
	Start, End int
}

func (ber *BracketEvalResult) isIndex() bool {
	return ber.Start == ber.End
}

// Eval evaluates an expression and returns a value.
func (v *ValuerEval) Eval(expr Expr) interface{} {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *BinaryExpr:
		return v.evalBinaryExpr(expr)
	//case *BooleanLiteral:
	//	return expr.Val
	case *IntegerLiteral:
		return expr.Val
	case *NumberLiteral:
		return expr.Val
	case *ParenExpr:
		return v.Eval(expr.Expr)
	case *StringLiteral:
		return expr.Val
	case *BooleanLiteral:
		return expr.Val
	case *ColonExpr:
		return &BracketEvalResult{Start: expr.Start, End: expr.End}
	case *IndexExpr:
		return &BracketEvalResult{Start: expr.Index, End: expr.Index}
	case *Call:
		if valuer, ok := v.Valuer.(CallValuer); ok {
			var args []interface{}
			if len(expr.Args) > 0 {
				args = make([]interface{}, len(expr.Args))
				for i, arg := range expr.Args {
					if aggreValuer, ok := valuer.(AggregateCallValuer); isAggFunc(expr) && ok {
						args[i] = aggreValuer.GetAllTuples().AggregateEval(arg, aggreValuer.GetSingleCallValuer())
					} else {
						args[i] = v.Eval(arg)
						if _, ok := args[i].(error); ok {
							return args[i]
						}
					}
				}
			}
			val, _ := valuer.Call(expr.Name, args)
			return val
		}
		return nil
	case *FieldRef:
		if expr.StreamName == "" {
			val, _ := v.Valuer.Value(expr.Name)
			return val
		} else {
			//The field specified with stream source
			val, _ := v.Valuer.Value(string(expr.StreamName) + COLUMN_SEPARATOR + expr.Name)
			return val
		}
	case *MetaRef:
		if expr.StreamName == "" {
			val, _ := v.Valuer.Meta(expr.Name)
			return val
		} else {
			//The field specified with stream source
			val, _ := v.Valuer.Meta(string(expr.StreamName) + COLUMN_SEPARATOR + expr.Name)
			return val
		}
	case *Wildcard:
		val, _ := v.Valuer.Value("")
		return val
	case *CaseExpr:
		return v.evalCase(expr)
	default:
		return nil
	}
}

func (v *ValuerEval) evalBinaryExpr(expr *BinaryExpr) interface{} {
	lhs := v.Eval(expr.LHS)
	switch val := lhs.(type) {
	case map[string]interface{}:
		return v.evalJsonExpr(val, expr.OP, expr.RHS)
	case Message:
		return v.evalJsonExpr(map[string]interface{}(val), expr.OP, expr.RHS)
	case error:
		return val
	}
	if isSliceOrArray(lhs) {
		return v.evalJsonExpr(lhs, expr.OP, expr.RHS)
	}
	rhs := v.Eval(expr.RHS)
	if _, ok := rhs.(error); ok {
		return rhs
	}
	return v.simpleDataEval(lhs, rhs, expr.OP)
}

func (v *ValuerEval) evalCase(expr *CaseExpr) interface{} {
	if expr.Value != nil { // compare value to all when clause
		ev := v.Eval(expr.Value)
		for _, w := range expr.WhenClauses {
			wv := v.Eval(w.Expr)
			switch r := v.simpleDataEval(ev, wv, EQ).(type) {
			case error:
				return fmt.Errorf("evaluate case expression error: %s", r)
			case bool:
				if r {
					return v.Eval(w.Result)
				}
			}
		}
	} else {
		for _, w := range expr.WhenClauses {
			switch r := v.Eval(w.Expr).(type) {
			case error:
				return fmt.Errorf("evaluate case expression error: %s", r)
			case bool:
				if r {
					return v.Eval(w.Result)
				}
			}
		}
	}
	if expr.ElseClause != nil {
		return v.Eval(expr.ElseClause)
	}
	return nil
}

func isSliceOrArray(v interface{}) bool {
	kind := reflect.ValueOf(v).Kind()
	return kind == reflect.Array || kind == reflect.Slice
}

func (v *ValuerEval) evalJsonExpr(result interface{}, op Token, expr Expr) interface{} {
	switch op {
	case ARROW:
		if val, ok := result.(map[string]interface{}); ok {
			switch e := expr.(type) {
			case *FieldRef, *MetaRef:
				ve := &ValuerEval{Valuer: Message(val)}
				return ve.Eval(e)
			default:
				return fmt.Errorf("the right expression is not a field reference node")
			}
		} else {
			return fmt.Errorf("the result %v is not a type of map[string]interface{}", result)
		}
	case SUBSET:
		if isSliceOrArray(result) {
			return v.subset(result, expr)
		} else {
			return fmt.Errorf("%v is an invalid operation for %T", op, result)
		}
	default:
		return fmt.Errorf("%v is an invalid operation for %T", op, result)
	}
}

func (v *ValuerEval) subset(result interface{}, expr Expr) interface{} {
	val := reflect.ValueOf(result)
	ber := v.Eval(expr)
	if berVal, ok1 := ber.(*BracketEvalResult); ok1 {
		if berVal.isIndex() {
			if 0 > berVal.Start {
				if 0 > berVal.Start+val.Len() {
					return fmt.Errorf("out of index: %d of %d", berVal.Start, val.Len())
				}
				berVal.Start += val.Len()
			} else if berVal.Start >= val.Len() {
				return fmt.Errorf("out of index: %d of %d", berVal.Start, val.Len())
			}
			return val.Index(berVal.Start).Interface()
		} else {
			if 0 > berVal.Start {
				if 0 > berVal.Start+val.Len() {
					return fmt.Errorf("out of index: %d of %d", berVal.Start, val.Len())
				}
				berVal.Start += val.Len()
			} else if berVal.Start >= val.Len() {
				return fmt.Errorf("start value is out of index: %d of %d", berVal.Start, val.Len())
			}
			if math.MinInt32 == berVal.End {
				berVal.End = val.Len()
			} else if 0 > berVal.End {
				if 0 > berVal.End+val.Len() {
					return fmt.Errorf("out of index: %d of %d", berVal.End, val.Len())
				}
				berVal.End += val.Len()
			} else if berVal.End > val.Len() {
				return fmt.Errorf("end value is out of index: %d of %d", berVal.End, val.Len())
			} else if berVal.Start >= berVal.End {
				return fmt.Errorf("start cannot be greater than end. start:%d  end:%d", berVal.Start, berVal.End)
			}
			return val.Slice(berVal.Start, berVal.End).Interface()
		}
	} else {
		return fmt.Errorf("invalid evaluation result - %v", berVal)
	}
}

//lhs and rhs are non-nil
func (v *ValuerEval) simpleDataEval(lhs, rhs interface{}, op Token) interface{} {
	if lhs == nil || rhs == nil {
		switch op {
		case EQ, LTE, GTE:
			if lhs == nil && rhs == nil {
				return true
			} else {
				return false
			}
		case NEQ:
			if lhs == nil && rhs == nil {
				return false
			} else {
				return true
			}
		case LT, GT:
			return false
		default:
			return nil
		}
	}
	lhs = convertNum(lhs)
	rhs = convertNum(rhs)
	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, ok := rhs.(bool)
		if !ok {
			return invalidOpError(lhs, op, rhs)
		}
		switch op {
		case AND:
			return lhs && rhs
		case OR:
			return lhs || rhs
		case BITWISE_AND:
			return lhs && rhs
		case BITWISE_OR:
			return lhs || rhs
		case BITWISE_XOR:
			return lhs != rhs
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case float64:
		// Try the rhs as a float64, int64, or uint64
		rhsf, ok := rhs.(float64)
		if !ok {
			switch val := rhs.(type) {
			case int64:
				rhsf, ok = float64(val), true
			case uint64:
				rhsf, ok = float64(val), true
			}
		}
		if !ok {
			return invalidOpError(lhs, op, rhs)
		}
		rhs := rhsf
		switch op {
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		case LT:
			return lhs < rhs
		case LTE:
			return lhs <= rhs
		case GT:
			return lhs > rhs
		case GTE:
			return lhs >= rhs
		case ADD:
			return lhs + rhs
		case SUB:
			return lhs - rhs
		case MUL:
			return lhs * rhs
		case DIV:
			if rhs == 0 {
				return fmt.Errorf("divided by zero")
			}
			return lhs / rhs
		case MOD:
			if rhs == 0 {
				return fmt.Errorf("divided by zero")
			}
			return math.Mod(lhs, rhs)
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case int64:
		// Try as a float64 to see if a float cast is required.
		switch rhs := rhs.(type) {
		case float64:
			lhs := float64(lhs)
			switch op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return math.Mod(lhs, rhs)
			default:
				return invalidOpError(lhs, op, rhs)
			}
		case int64:
			switch op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if v.IntegerFloatDivision {
					if rhs == 0 {
						return fmt.Errorf("divided by zero")
					}
					return float64(lhs) / float64(rhs)
				}

				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs % rhs
			case BITWISE_AND:
				return lhs & rhs
			case BITWISE_OR:
				return lhs | rhs
			case BITWISE_XOR:
				return lhs ^ rhs
			default:
				return invalidOpError(lhs, op, rhs)
			}
		case uint64:
			switch op {
			case EQ:
				return uint64(lhs) == rhs
			case NEQ:
				return uint64(lhs) != rhs
			case LT:
				if lhs < 0 {
					return true
				}
				return uint64(lhs) < rhs
			case LTE:
				if lhs < 0 {
					return true
				}
				return uint64(lhs) <= rhs
			case GT:
				if lhs < 0 {
					return false
				}
				return uint64(lhs) > rhs
			case GTE:
				if lhs < 0 {
					return false
				}
				return uint64(lhs) >= rhs
			case ADD:
				return uint64(lhs) + rhs
			case SUB:
				return uint64(lhs) - rhs
			case MUL:
				return uint64(lhs) * rhs
			case DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return uint64(lhs) / rhs
			case MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return uint64(lhs) % rhs
			case BITWISE_AND:
				return uint64(lhs) & rhs
			case BITWISE_OR:
				return uint64(lhs) | rhs
			case BITWISE_XOR:
				return uint64(lhs) ^ rhs
			default:
				return invalidOpError(lhs, op, rhs)
			}
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case uint64:
		// Try as a float64 to see if a float cast is required.
		switch rhs := rhs.(type) {
		case float64:
			lhs := float64(lhs)
			switch op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return math.Mod(lhs, rhs)
			default:
				return invalidOpError(lhs, op, rhs)
			}
		case int64:
			switch op {
			case EQ:
				return lhs == uint64(rhs)
			case NEQ:
				return lhs != uint64(rhs)
			case LT:
				if rhs < 0 {
					return false
				}
				return lhs < uint64(rhs)
			case LTE:
				if rhs < 0 {
					return false
				}
				return lhs <= uint64(rhs)
			case GT:
				if rhs < 0 {
					return true
				}
				return lhs > uint64(rhs)
			case GTE:
				if rhs < 0 {
					return true
				}
				return lhs >= uint64(rhs)
			case ADD:
				return lhs + uint64(rhs)
			case SUB:
				return lhs - uint64(rhs)
			case MUL:
				return lhs * uint64(rhs)
			case DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / uint64(rhs)
			case MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs % uint64(rhs)
			case BITWISE_AND:
				return lhs & uint64(rhs)
			case BITWISE_OR:
				return lhs | uint64(rhs)
			case BITWISE_XOR:
				return lhs ^ uint64(rhs)
			default:
				return invalidOpError(lhs, op, rhs)
			}
		case uint64:
			switch op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return fmt.Errorf("divided by zero")
				}
				return lhs % rhs
			case BITWISE_AND:
				return lhs & rhs
			case BITWISE_OR:
				return lhs | rhs
			case BITWISE_XOR:
				return lhs ^ rhs
			default:
				return invalidOpError(lhs, op, rhs)
			}
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case string:
		rhss, ok := rhs.(string)
		if !ok {
			return invalidOpError(lhs, op, rhs)
		}
		switch op {
		case EQ:
			return lhs == rhss
		case NEQ:
			return lhs != rhss
		case LT:
			return lhs < rhss
		case LTE:
			return lhs <= rhss
		case GT:
			return lhs > rhss
		case GTE:
			return lhs >= rhss
		default:
			return invalidOpError(lhs, op, rhs)
		}
	case time.Time:
		rt, err := common.InterfaceToTime(rhs, "")
		if err != nil {
			return invalidOpError(lhs, op, rhs)
		}
		switch op {
		case EQ:
			return lhs.Equal(rt)
		case NEQ:
			return !lhs.Equal(rt)
		case LT:
			return lhs.Before(rt)
		case LTE:
			return lhs.Before(rt) || lhs.Equal(rt)
		case GT:
			return lhs.After(rt)
		case GTE:
			return lhs.After(rt) || lhs.Equal(rt)
		default:
			return invalidOpError(lhs, op, rhs)
		}
	default:
		return invalidOpError(lhs, op, rhs)
	}

	return invalidOpError(lhs, op, rhs)
}

func invalidOpError(lhs interface{}, op Token, rhs interface{}) error {
	return fmt.Errorf("invalid operation %[1]T(%[1]v) %s %[3]T(%[3]v)", lhs, tokens[op], rhs)
}

func convertNum(para interface{}) interface{} {
	if isInt(para) {
		para = toInt64(para)
	} else if isFloat(para) {
		para = toFloat64(para)
	}
	return para
}

func isInt(para interface{}) bool {
	switch para.(type) {
	case int:
		return true
	case int8:
		return true
	case int16:
		return true
	case int32:
		return true
	case int64:
		return true
	}
	return false
}

func toInt64(para interface{}) int64 {
	if v, ok := para.(int); ok {
		return int64(v)
	} else if v, ok := para.(int8); ok {
		return int64(v)
	} else if v, ok := para.(int16); ok {
		return int64(v)
	} else if v, ok := para.(int32); ok {
		return int64(v)
	} else if v, ok := para.(int64); ok {
		return v
	}
	return 0
}

func isFloat(para interface{}) bool {
	switch para.(type) {
	case float32:
		return true
	case float64:
		return true
	}
	return false
}

func toFloat64(para interface{}) float64 {
	if v, ok := para.(float32); ok {
		return float64(v)
	} else if v, ok := para.(float64); ok {
		return v
	}
	return 0
}

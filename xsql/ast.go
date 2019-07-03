package xsql

import (
	"engine/common"
	"fmt"
	"log"
	"math"
	"strings"
)



type Node interface {
	node()
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

func (ss Sources) node(){}

type Table struct {
	Name string
	Alias string
}

func (t *Table) source() {}
func (ss *Table) node(){}


type JoinType int
const (
	LEFT_JOIN JoinType = iota
	INNER_JOIN
)

type Join struct {
	Name     string
	Alias    string
	JoinType JoinType
	Expr     Expr
}

func (j *Join) source() {}
func (ss *Join) node(){}

type Joins []Join
func (ss Joins) node(){}

type Statement interface{
	Stmt()
	Node
}

type SelectStatement struct {
	Fields    Fields
	Sources Sources
	Joins Joins
	Condition Expr
	Dimensions Dimensions
	SortFields SortFields
}

func (ss *SelectStatement) Stmt() {}
func (ss *SelectStatement) node(){}

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
	End int
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
	Name string
	Ascending bool
}

type SortFields []SortField

type Dimensions []Dimension

func (f *Field) expr() {}
func (f *Field) node(){}

func (pe *ParenExpr) expr() {}
func (pe *ParenExpr) node(){}

func (ae *ArrowExpr) expr() {}
func (ae *ArrowExpr) node(){}

func (be *BracketExpr) expr() {}
func (be *BracketExpr) node(){}

func (be *ColonExpr) expr() {}
func (be *ColonExpr) node(){}

func (be *IndexExpr) expr() {}
func (be *IndexExpr) node(){}

func (w *Wildcard) expr() {}
func (w *Wildcard) node(){}

func (bl *BooleanLiteral) expr()    {}
func (bl *BooleanLiteral) literal() {}
func (bl *BooleanLiteral) node(){}

func (tl *TimeLiteral) expr()    {}
func (tl *TimeLiteral) literal() {}
func (tl *TimeLiteral) node(){}

func (il *IntegerLiteral) expr()    {}
func (il *IntegerLiteral) literal() {}
func (il *IntegerLiteral) node(){}

func (nl *NumberLiteral) expr()    {}
func (nl *NumberLiteral) literal() {}
func (nl *NumberLiteral) node(){}

func (sl *StringLiteral) expr()    {}
func (sl *StringLiteral) literal() {}
func (sl *StringLiteral) node(){}

func (d *Dimension) expr() {}
func (d *Dimension) node(){}

func (d Dimensions) node(){}

func (sf *SortField) expr() {}
func (sf *SortField) node(){}

func (sf SortFields) node(){}

type Call struct {
	Name string
	Args []Expr
}

func (c *Call) expr() {}
func (c *Call) literal() {}
func (c *Call) node(){}

type WindowType int

const (
	NOT_WINDOW WindowType = iota
	TUMBLING_WINDOW
	HOPPING_WINDOW
	SLIDING_WINDOW
	SESSION_WINDOW
)

type Windows struct {
	Args []Expr
	WindowType WindowType
}

func (w *Windows) expr() {}
func (w *Windows) literal() {}
func (w *Windows) node(){}

type  SelectStatements []SelectStatement

func (ss *SelectStatements) node(){}

type Fields []Field
func (fs Fields) node(){}

type BinaryExpr struct {
	OP Token
	LHS Expr
	RHS Expr
}

func (fe *BinaryExpr) expr() {}
func (be *BinaryExpr) node(){}

type FieldRef struct {
	StreamName StreamName
	Name  string
}

func (fr *FieldRef) expr() {}
func (fr *FieldRef) node(){}


// The stream AST tree
type Options map[string]string
func (o Options) node() {}

type StreamName string
func (sn *StreamName) node() {}

type StreamStmt struct {
	Name StreamName
	StreamFields StreamFields
	Options Options
}

func (ss *StreamStmt) node(){}
func (ss *StreamStmt) Stmt() {}


type FieldType interface {
	fieldType()
	Node
}

type StreamField struct {
	Name string
	FieldType
}

type StreamFields []StreamField

func (sf StreamFields) node(){}

type BasicType struct {
	Type DataType
}
func (bt *BasicType) fieldType() {}
func (bt *BasicType) node(){}

type ArrayType struct {
	Type DataType
	FieldType
}
func (at *ArrayType) fieldType() {}
func (at *ArrayType) node(){}

type RecType struct {
	StreamFields StreamFields
}
func (rt *RecType) fieldType() {}
func (rt *RecType) node(){}

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
func (ss *ShowStreamsStatement) node(){}

func (dss *DescribeStreamStatement) Stmt() {}
func (dss *DescribeStreamStatement) node(){}

func (ess *ExplainStreamStatement) Stmt() {}
func (ess *ExplainStreamStatement) node(){}

func (dss *DropStreamStatement) Stmt() {}
func (dss *DropStreamStatement) node(){}


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

	case *Windows:
		for _, expr := range n.Args {
			Walk(v, expr)
		}

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

	case *StreamStmt:
		Walk(v, &n.Name)
		Walk(v, n.StreamFields)
		Walk(v, n.Options)

	case *BasicType:
		Walk(v, n)

	case *ArrayType:
		Walk(v, n)

	case *RecType:
		Walk(v, n)

	case *ShowStreamsStatement:
		Walk(v, n)

	case *DescribeStreamStatement:
		Walk(v, n)

	case *ExplainStreamStatement:
		Walk(v, n)

	case *DropStreamStatement:
		Walk(v, n)
	}
}


// Valuer is the interface that wraps the Value() method.
type Valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key string) (interface{}, bool)
}

// CallValuer implements the Call method for evaluating function calls.
type CallValuer interface {
	Valuer

	// Call is invoked to evaluate a function call (if possible).
	Call(name string, args []interface{}) (interface{}, bool)
}

// MapValuer is a valuer that substitutes values for the mapped interface.
type MapValuer map[string]interface{}

// Value returns the value for a key in the MapValuer.
func (m MapValuer) Value(key string) (interface{}, bool) {
	if keys := strings.Split(key, "."); len(keys) == 1 {
		v, ok := m[key]
		return v, ok
	} else if len(keys) == 2 {
		v, ok := m[keys[1]]
		return v, ok
	}
	common.Log.Println("Invalid key: " + key + ", expect source.field or field.")
	return nil, false
}

type WildcardValuer struct {
	Data map[string]interface{}
}

func (wv *WildcardValuer) Value(key string) (interface{}, bool) {
	//TODO Need to read the schema from stream, and fill into the map
	return wv.Data, true
}


type EmitterTuple struct {
	Emitter string
	Message map[string]interface{}
}

type MergedEmitterTuple struct {
	MergedMessage []EmitterTuple
}

func (me *MergedEmitterTuple) AddMergedMessage(message EmitterTuple) {
	me.MergedMessage = append(me.MergedMessage, message)
}

type MergedEmitterTupleSets []MergedEmitterTuple


type Messages []map[string]interface{}

type EmitterTuples struct {
	Emitter string
	Messages Messages
}

type EvalResultAndMessage struct {
	Stream string
	Result interface{}
	Message map[string]interface{}
}

type ResultsAndMessages []EvalResultAndMessage

type MultiEmitterTuples []EmitterTuples

func (met *MultiEmitterTuples) GetBySrc(src string) Messages {
	for _, me := range *met {
		if me.Emitter == src {
			return me.Messages
		}
	}
	return nil
}

type Tuple struct {
	EmitterName string
	Message interface{}
	Timestamp int64
}

func (met *MultiEmitterTuples) addTuple(tuple *Tuple) {
	found := false
	m, ok := tuple.Message.(map[string]interface{})
	if !ok {
		log.Printf("Expect map[string]interface{} for the message type.")
		return
	}

	for _, t := range *met {
		if t.Emitter == tuple.EmitterName {
			t.Messages = append(t.Messages, m)
			break
		}
	}

	if !found {
		ets := &EmitterTuples{Emitter:tuple.EmitterName}
		ets.Messages = append(ets.Messages, m)
		*met = append(*met, *ets)
	}
}

func (met MultiEmitterTuples) Value(key string) (interface{}, bool) {
	var ret ResultsAndMessages
	if keys := strings.Split(key, "."); len(keys) != 2 {
		common.Log.Infoln("Wrong key: ", key, ", expect dot in the expression.")
		return nil, false
	} else {
		emitter, key := keys[0], keys[1]
		for _, me := range met {
			if me.Emitter == emitter {
				for _, m := range me.Messages {
					if r, ok := m[key]; ok {
						rm := &EvalResultAndMessage{Stream: keys[0], Result:r, Message:m}
						ret = append(ret, *rm)
					}
				}
				break
			}
		}
	}

	if len(ret) > 0 {
		return ret, true
	} else {
		return nil, false
	}
}

func (met *MultiEmitterTuples) AddTuple(tuple *Tuple) {
	found := false
	m, ok := tuple.Message.(map[string]interface{})
	if !ok {
		common.Log.Printf("Expect map[string]interface{} for the message type.")
		return
	}

	for _, t := range *met {
		if t.Emitter == tuple.EmitterName {
			t.Messages = append(t.Messages, m)
			break
		}
	}

	if !found {
		ets := &EmitterTuples{Emitter:tuple.EmitterName}
		ets.Messages = append(ets.Messages, m)
		*met = append(*met, *ets)
	}
}

// Eval evaluates expr against a map.
func Eval(expr Expr, m map[string]interface{}) interface{} {
	eval := ValuerEval{Valuer: MapValuer(m)}
	return eval.Eval(expr)
}

// ValuerEval will evaluate an expression using the Valuer.
type ValuerEval struct {
	Valuer Valuer

	// IntegerFloatDivision will set the eval system to treat
	// a division between two integers as a floating point division.
	IntegerFloatDivision bool
	JoinType JoinType
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

func (a multiValuer) Call(name string, args []interface{}) (interface{}, bool) {
	for _, valuer := range a {
		if valuer, ok := valuer.(CallValuer); ok {
			if v, ok := valuer.Call(name, args); ok {
				return v, true
			}
		}
	}
	return nil, false
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
	case *ColonExpr:
		return &BracketEvalResult{Start:expr.Start, End:expr.End}
	case *IndexExpr:
		return &BracketEvalResult{Start:expr.Index, End:expr.Index}
	case *Call:
		if valuer, ok := v.Valuer.(CallValuer); ok {
			var args []interface{}
			if len(expr.Args) > 0 {
				args = make([]interface{}, len(expr.Args))
				for i := range expr.Args {
					args[i] = v.Eval(expr.Args[i])
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
			val, _ := v.Valuer.Value(string(expr.StreamName) + "." + expr.Name)
			return val
		}
	case *Wildcard:
		val, _ := v.Valuer.Value("")
		return val
	default:
		return nil
	}
	return nil
}


func (v *ValuerEval) evalBinaryExpr(expr *BinaryExpr) interface{} {
	lhs := v.Eval(expr.LHS)
	switch val := lhs.(type) {
	case ResultsAndMessages:
		return v.evalSet(val, expr)
	case map[string]interface{}:
		return v.evalJsonExpr(val, expr.OP, expr.RHS)
	case []interface{}:
		return v.evalJsonExpr(val, expr.OP, expr.RHS)
	}

	rhs := v.Eval(expr.RHS)
	if lhs == nil && rhs != nil {
		// When the LHS is nil and the RHS is a boolean, implicitly cast the
		// nil to false.
		if _, ok := rhs.(bool); ok {
			lhs = false
		}
	} else if lhs != nil && rhs == nil {
		// Implicit cast of the RHS nil to false when the LHS is a boolean.
		if _, ok := lhs.(bool); ok {
			rhs = false
		}
	}


	return v.simpleDataEval(lhs, rhs, expr.OP)
}


func (v *ValuerEval) evalJsonExpr(result interface{}, op Token,  expr Expr) interface{} {
	if val, ok := result.(map[string]interface{}); ok {
		switch op {
		case ARROW:
			if exp, ok := expr.(*FieldRef); ok {
				ve := &ValuerEval{Valuer: MapValuer(val)}
				return ve.Eval(exp)
			} else {
				fmt.Printf("The right expression is not a field reference node.\n")
				return nil
			}
		default:
			fmt.Printf("%v is an invalid operation.\n", op)
			return nil
		}
	}

	if val, ok := result.([]interface{}); ok {
		switch op {
		case SUBSET:
			ber := v.Eval(expr)
			if berVal, ok1 := ber.(*BracketEvalResult); ok1 {
				if berVal.isIndex() {
					if berVal.Start >= len(val) {
						fmt.Printf("Out of index: %d of %d.\n", berVal.Start, len(val))
						return nil
					}
					return val[berVal.Start]
				} else {
					if berVal.Start >= len(val) {
						fmt.Printf("Start value is out of index: %d of %d.\n", berVal.Start, len(val))
						return nil
					}

					if berVal.End >= len(val) {
						fmt.Printf("End value is out of index: %d of %d.\n", berVal.End, len(val))
						return nil
					}
					return val[berVal.Start : berVal.End]
				}
			} else {
				fmt.Printf("Invalid evaluation result - %v.\n", berVal)
				return nil
			}
		default:
			fmt.Printf("%v is an invalid operation.\n", op)
			return nil
		}
	}
	return nil
}

func (v *ValuerEval) evalSet(lefts ResultsAndMessages, expr *BinaryExpr) interface{} {
	//For the JSON expressions
	if expr.OP == ARROW || expr.OP == SUBSET {
		for i, left := range lefts {
			lefts[i].Result = v.evalJsonExpr(left.Result, expr.OP, expr.RHS)
		}
		return lefts
	}

	//For the simple type expressions
	rhs := v.Eval(expr.RHS)
	rights, ok := rhs.(ResultsAndMessages)
	if rhs != nil && !ok {
		for i, left := range lefts {
			r := v.simpleDataEval(left.Result, rhs, expr.OP)
			lefts[i].Result = r
		}
		return lefts
	}

	sets := MergedEmitterTupleSets{}
	for _, left := range lefts {
		merged := &MergedEmitterTuple{}
		lm := &EmitterTuple{string(left.Stream), left.Message}
		if v.JoinType == LEFT_JOIN {
			merged.AddMergedMessage(*lm)
		}

		innerAppend := false
		for _, right := range rights {
			r := v.simpleDataEval(left.Result, right.Result, expr.OP)
			if v1, ok := r.(bool); ok {
				if v1 {
					if v.JoinType == INNER_JOIN && !innerAppend{
						merged.AddMergedMessage(*lm)
						innerAppend = true
					}
					rm := &EmitterTuple{string(right.Stream), right.Message}
					merged.AddMergedMessage(*rm)
				}
			} else {
				common.Log.Infoln("Evaluation error for set.")
			}
		}
		if len(merged.MergedMessage) > 0 {
			sets = append(sets, *merged)
		}
	}

	return sets
}


func (v *ValuerEval) simpleDataEval(lhs, rhs interface{}, op Token) interface{} {
	lhs = convertNum(lhs)
	rhs = convertNum(rhs)
	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, ok := rhs.(bool)
		switch op {
		case AND:
			return ok && (lhs && rhs)
		case OR:
			return ok && (lhs || rhs)
		case BITWISE_AND:
			return ok && (lhs && rhs)
		case BITWISE_OR:
			return ok && (lhs || rhs)
		case BITWISE_XOR:
			return ok && (lhs != rhs)
		case EQ:
			return ok && (lhs == rhs)
		case NEQ:
			return ok && (lhs != rhs)
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

		rhs := rhsf
		switch op {
		case EQ:
			return ok && (lhs == rhs)
		case NEQ:
			return ok && (lhs != rhs)
		case LT:
			return ok && (lhs < rhs)
		case LTE:
			return ok && (lhs <= rhs)
		case GT:
			return ok && (lhs > rhs)
		case GTE:
			return ok && (lhs >= rhs)
		case ADD:
			if !ok {
				return nil
			}
			return lhs + rhs
		case SUB:
			if !ok {
				return nil
			}
			return lhs - rhs
		case MUL:
			if !ok {
				return nil
			}
			return lhs * rhs
		case DIV:
			if !ok {
				return nil
			} else if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		case MOD:
			if !ok {
				return nil
			}
			return math.Mod(lhs, rhs)
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
					return float64(0)
				}
				return lhs / rhs
			case MOD:
				return math.Mod(lhs, rhs)
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
						return float64(0)
					}
					return float64(lhs) / float64(rhs)
				}

				if rhs == 0 {
					return int64(0)
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return int64(0)
				}
				return lhs % rhs
			case BITWISE_AND:
				return lhs & rhs
			case BITWISE_OR:
				return lhs | rhs
			case BITWISE_XOR:
				return lhs ^ rhs
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
					return uint64(0)
				}
				return uint64(lhs) / rhs
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return uint64(lhs) % rhs
			case BITWISE_AND:
				return uint64(lhs) & rhs
			case BITWISE_OR:
				return uint64(lhs) | rhs
			case BITWISE_XOR:
				return uint64(lhs) ^ rhs
			}
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
					return float64(0)
				}
				return lhs / rhs
			case MOD:
				return math.Mod(lhs, rhs)
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
					return uint64(0)
				}
				return lhs / uint64(rhs)
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs % uint64(rhs)
			case BITWISE_AND:
				return lhs & uint64(rhs)
			case BITWISE_OR:
				return lhs | uint64(rhs)
			case BITWISE_XOR:
				return lhs ^ uint64(rhs)
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
					return uint64(0)
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs % rhs
			case BITWISE_AND:
				return lhs & rhs
			case BITWISE_OR:
				return lhs | rhs
			case BITWISE_XOR:
				return lhs ^ rhs
			}
		}
	case string:
		switch op {
		case EQ:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs == rhs
		case NEQ:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs != rhs
		}
	}

	// The types were not comparable. If our operation was an equality operation,
	// return false instead of true.
	switch op {
	case EQ, NEQ, LT, LTE, GT, GTE:
		return false
	}
	return nil
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
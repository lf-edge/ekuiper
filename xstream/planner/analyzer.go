package planner

import (
	"fmt"
	"github.com/emqx/kuiper/common/kv"
	"github.com/emqx/kuiper/xsql"
	"sort"
	"strings"
)

type aliasInfo struct {
	alias       xsql.Field
	refSources  []string
	isAggregate *bool
}

// Analyze the select statement by decorating the info from stream statement.
// Typically, set the correct stream name for fieldRefs
func decorateStmt(s *xsql.SelectStatement, store kv.KeyValue) ([]*xsql.StreamStmt, map[string]*aliasInfo, error) {
	streamsFromStmt := xsql.GetStreams(s)
	streamStmts := make([]*xsql.StreamStmt, len(streamsFromStmt))
	aliasSourceMap := make(map[string]*aliasInfo)
	isSchemaless := false
	for i, s := range streamsFromStmt {
		streamStmt, err := xsql.GetDataSource(store, s)
		if err != nil {
			return nil, nil, fmt.Errorf("fail to get stream %s, please check if stream is created", s)
		}
		streamStmts[i] = streamStmt
		if streamStmt.StreamFields == nil {
			isSchemaless = true
		}
	}
	var walkErr error
	for _, f := range s.Fields {
		if f.AName != "" {
			if _, ok := aliasSourceMap[strings.ToLower(f.AName)]; ok {
				return nil, nil, fmt.Errorf("duplicate alias %s", f.AName)
			}
			refStreams := make(map[string]struct{})
			xsql.WalkFunc(f.Expr, func(n xsql.Node) {
				switch expr := n.(type) {
				case *xsql.FieldRef:
					err := updateFieldRefStream(expr, streamStmts, isSchemaless)
					if err != nil {
						walkErr = err
						return
					}
					if expr.StreamName != "" {
						refStreams[string(expr.StreamName)] = struct{}{}
					}
				}
			})
			if walkErr != nil {
				return nil, nil, walkErr
			}
			refStreamKeys := make([]string, len(refStreams))
			c := 0
			for k, _ := range refStreams {
				refStreamKeys[c] = k
				c++
			}
			aliasSourceMap[strings.ToLower(f.AName)] = &aliasInfo{
				alias:      f,
				refSources: refStreamKeys,
			}
		}
	}
	// second phase to check the alias aggregate status
	for _, v := range aliasSourceMap {
		if v.isAggregate == nil {
			tr := isAggregate(v.alias.Expr, aliasSourceMap)
			v.isAggregate = &tr
		}
	}
	// Select fields are visited firstly to make sure all aliases have streamName set
	xsql.WalkFunc(s, func(n xsql.Node) {
		//skip alias field
		switch f := n.(type) {
		case *xsql.Field:
			if f.AName != "" {
				return
			}
		case *xsql.FieldRef:
			if f.StreamName == xsql.DEFAULT_STREAM {
				for aname, ainfo := range aliasSourceMap {
					if strings.EqualFold(f.Name, aname) {
						switch len(ainfo.refSources) {
						case 0: // if no ref source, we can put it to any stream, here just assign it to the first stream
							f.StreamName = streamStmts[0].Name
						case 1:
							f.StreamName = xsql.StreamName(ainfo.refSources[0])
						default:
							f.StreamName = xsql.MULTI_STREAM
						}
						return
					}

				}
			}
			err := updateFieldRefStream(f, streamStmts, isSchemaless)
			if err != nil {
				walkErr = err
			}
		}
	})
	if walkErr == nil {
		walkErr = validate(s, aliasSourceMap)
	}
	return streamStmts, aliasSourceMap, walkErr
}

func validate(s *xsql.SelectStatement, sourceMap map[string]*aliasInfo) (err error) {
	if isAggregate(s.Condition, sourceMap) {
		return fmt.Errorf("Not allowed to call aggregate functions in WHERE clause.")
	}
	if !allAggregate(s.Having, sourceMap) {
		return fmt.Errorf("Not allowed to call non-aggregate functions in HAVING clause.")
	}
	for _, d := range s.Dimensions {
		if isAggregate(d.Expr, sourceMap) {
			return fmt.Errorf("Not allowed to call aggregate functions in GROUP BY clause.")
		}
	}
	xsql.WalkFunc(s, func(n xsql.Node) {
		switch f := n.(type) {
		case *xsql.Call:
			// aggregate call should not have any aggregate arg
			if xsql.IsAggFunc(f) {
				for _, arg := range f.Args {
					tr := isAggregate(arg, sourceMap)
					if tr {
						err = fmt.Errorf("invalid argument for func %s: aggregate argument is not allowed", f.Name)
						return
					}
				}
			}
		}
	})
	return
}

func complexAlias(aliasMap map[string]*aliasInfo) (aggregateAlias xsql.Fields, joinAlias xsql.Fields) {
	for _, ainfo := range aliasMap {
		if *ainfo.isAggregate {
			aggregateAlias = append(aggregateAlias, ainfo.alias)
			continue
		}
		if len(ainfo.refSources) > 1 {
			joinAlias = append(joinAlias, ainfo.alias)
		}
	}
	return
}

// file-private functions below

// isAggregate check if an expression is aggregate with the binding alias info
func isAggregate(expr xsql.Expr, sourceMap map[string]*aliasInfo) (r bool) {
	xsql.WalkFunc(expr, func(n xsql.Node) {
		switch f := n.(type) {
		case *xsql.Field:
			if f.AName != "" {
				r = true
				return
			}
		case *xsql.Call:
			if ok := xsql.IsAggFunc(f); ok {
				r = true
				return
			}
		case *xsql.FieldRef:
			if v, ok := sourceMap[strings.ToLower(f.Name)]; ok {
				if v.isAggregate == nil {
					tr := isAggregate(v.alias.Expr, sourceMap)
					if tr {
						r = tr
						return
					}
				} else if *v.isAggregate {
					r = true
					return
				}
			}
		}
	})
	return
}

// allAggregate checks if all expressions of binary expression are aggregate
func allAggregate(expr xsql.Expr, sourceMap map[string]*aliasInfo) (r bool) {
	r = true
	xsql.WalkFunc(expr, func(n xsql.Node) {
		switch f := expr.(type) {
		case *xsql.BinaryExpr:
			switch f.OP {
			case xsql.SUBSET, xsql.ARROW:
				// do nothing
			default:
				r = allAggregate(f.LHS, sourceMap) && allAggregate(f.RHS, sourceMap)
				return
			}
		case *xsql.Call, *xsql.FieldRef:
			if !isAggregate(f, sourceMap) {
				r = false
				return
			}
		}
	})
	return
}

func updateFieldRefStream(f *xsql.FieldRef, streamStmts []*xsql.StreamStmt, isSchemaless bool) (err error) {
	count := 0
	for _, streamStmt := range streamStmts {
		for _, field := range streamStmt.StreamFields {
			if strings.EqualFold(f.Name, field.Name) {
				if f.StreamName == xsql.DEFAULT_STREAM {
					f.StreamName = streamStmt.Name
					count++
				} else if f.StreamName == streamStmt.Name {
					count++
				}
				break
			}
		}
	}
	if count > 1 {
		err = fmt.Errorf("ambiguous field %s", f.Name)
	} else if count == 0 && f.StreamName == xsql.DEFAULT_STREAM { // alias may refer to non stream field
		if !isSchemaless {
			err = fmt.Errorf("unknown field %s.%s", f.StreamName, f.Name)
		} else if len(streamStmts) == 1 { // If only one schemaless stream, all the fields must be a field of that stream
			f.StreamName = streamStmts[0].Name
		}
	}
	return
}

func aliasFieldsForSource(aliasMap map[string]*aliasInfo, name xsql.StreamName, isFirst bool) (result xsql.Fields) {
	for _, ainfo := range aliasMap {
		if *ainfo.isAggregate {
			continue
		}
		switch len(ainfo.refSources) {
		case 0:
			if isFirst {
				result = append(result, ainfo.alias)
			}
		case 1:
			if strings.EqualFold(ainfo.refSources[0], string(name)) {
				result = append(result, ainfo.alias)
			}
		}
	}
	// sort to get a constant result for testing
	sort.Sort(result)
	return
}

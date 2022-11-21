// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"strings"
)

// Analyze the select statement by decorating the info from stream statement.
// Typically, set the correct stream name for fieldRefs
func decorateStmt(s *ast.SelectStatement, store kv.KeyValue) ([]*ast.StreamStmt, []*ast.Call, error) {
	streamsFromStmt := xsql.GetStreams(s)
	streamStmts := make([]*ast.StreamStmt, len(streamsFromStmt))
	isSchemaless := false
	for i, s := range streamsFromStmt {
		streamStmt, err := xsql.GetDataSource(store, s)
		if err != nil {
			return nil, nil, fmt.Errorf("fail to get stream %s, please check if stream is created", s)
		}
		streamStmts[i] = streamStmt
		// TODO fine grain control of schemaless
		if streamStmt.StreamFields == nil {
			isSchemaless = true
		}
	}

	dsn := ast.DefaultStream
	if len(streamsFromStmt) == 1 {
		dsn = streamStmts[0].Name
	}
	// [fieldName][streamsName][*aliasRef] if alias, with special key alias/default. Each key has exactly one value
	fieldsMap := newFieldsMap(isSchemaless, dsn)
	if !isSchemaless {
		for _, streamStmt := range streamStmts {
			for _, field := range streamStmt.StreamFields {
				fieldsMap.reserve(field.Name, streamStmt.Name)
			}
		}
	}
	var (
		walkErr       error
		aliasFields   []*ast.Field
		analyticFuncs []*ast.Call
	)
	// Scan columns fields: bind all field refs, collect alias
	for i, f := range s.Fields {
		ast.WalkFunc(f.Expr, func(n ast.Node) bool {
			switch f := n.(type) {
			case *ast.FieldRef:
				walkErr = fieldsMap.bind(f)
			}
			return true
		})
		if walkErr != nil {
			return nil, nil, walkErr
		}
		if f.AName != "" {
			aliasFields = append(aliasFields, &s.Fields[i])
		}
	}
	// bind alias field expressions
	for _, f := range aliasFields {
		ar, err := ast.NewAliasRef(f.Expr)
		if err != nil {
			walkErr = err
		} else {
			f.Expr = &ast.FieldRef{
				StreamName: ast.AliasStream,
				Name:       f.AName,
				AliasRef:   ar,
			}
			walkErr = fieldsMap.save(f.AName, ast.AliasStream, ar)
		}
	}
	// Bind field ref for alias AND set StreamName for all field ref
	ast.WalkFunc(s, func(n ast.Node) bool {
		switch f := n.(type) {
		case ast.Fields: // do not bind selection fields, should have done above
			return false
		case *ast.FieldRef:
			if f.StreamName != "" && f.StreamName != ast.DefaultStream {
				// check if stream exists
				found := false
				for _, sn := range streamsFromStmt {
					if sn == string(f.StreamName) {
						found = true
						break
					}
				}
				if !found {
					walkErr = fmt.Errorf("stream %s not found", f.StreamName)
					return true
				}
			}
			walkErr = fieldsMap.bind(f)
		}
		return true
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	walkErr = validate(s)
	// Collect all analytic function calls so that we can let them run firstly
	ast.WalkFunc(s, func(n ast.Node) bool {
		switch f := n.(type) {
		case ast.Fields:
			return false
		case *ast.Call:
			if function.IsAnalyticFunc(f.Name) {
				f.CachedField = fmt.Sprintf("%s_%s_%d", function.AnalyticPrefix, f.Name, f.FuncId)
				f.Cached = true
				analyticFuncs = append(analyticFuncs, &ast.Call{
					Name:        f.Name,
					FuncId:      f.FuncId,
					FuncType:    f.FuncType,
					Args:        f.Args,
					CachedField: f.CachedField,
					Partition:   f.Partition,
				})
			}
		}
		return true
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	// walk sources at last to let them run firstly
	// because other clause may depend on the alias defined here
	ast.WalkFunc(s.Fields, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			if function.IsAnalyticFunc(f.Name) {
				f.CachedField = fmt.Sprintf("%s_%s_%d", function.AnalyticPrefix, f.Name, f.FuncId)
				f.Cached = true
				analyticFuncs = append(analyticFuncs, &ast.Call{
					Name:        f.Name,
					FuncId:      f.FuncId,
					FuncType:    f.FuncType,
					Args:        f.Args,
					CachedField: f.CachedField,
					Partition:   f.Partition,
				})
			}
		}
		return true
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	return streamStmts, analyticFuncs, walkErr
}

func validate(s *ast.SelectStatement) (err error) {
	if xsql.IsAggregate(s.Condition) {
		return fmt.Errorf("Not allowed to call aggregate functions in WHERE clause.")
	}
	if !allAggregate(s.Having) {
		return fmt.Errorf("Not allowed to call non-aggregate functions in HAVING clause.")
	}
	for _, d := range s.Dimensions {
		if xsql.IsAggregate(d.Expr) {
			return fmt.Errorf("Not allowed to call aggregate functions in GROUP BY clause.")
		}
	}
	ast.WalkFunc(s, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			// aggregate call should not have any aggregate arg
			if function.IsAggFunc(f.Name) {
				for _, arg := range f.Args {
					tr := xsql.IsAggregate(arg)
					if tr {
						err = fmt.Errorf("invalid argument for func %s: aggregate argument is not allowed", f.Name)
						return false
					}
				}
			}
		}
		return true
	})
	return
}

// file-private functions below
// allAggregate checks if all expressions of binary expression are aggregate
func allAggregate(expr ast.Expr) (r bool) {
	r = true
	ast.WalkFunc(expr, func(n ast.Node) bool {
		switch f := expr.(type) {
		case *ast.BinaryExpr:
			switch f.OP {
			case ast.SUBSET, ast.ARROW:
				// do nothing
			default:
				r = allAggregate(f.LHS) && allAggregate(f.RHS)
				return false
			}
		case *ast.Call, *ast.FieldRef:
			if !xsql.IsAggregate(f) {
				r = false
				return false
			}
		}
		return true
	})
	return
}

type fieldsMap struct {
	content       map[string]streamFieldStore
	isSchemaless  bool
	defaultStream ast.StreamName
}

func newFieldsMap(isSchemaless bool, defaultStream ast.StreamName) *fieldsMap {
	return &fieldsMap{content: make(map[string]streamFieldStore), isSchemaless: isSchemaless, defaultStream: defaultStream}
}

func (f *fieldsMap) reserve(fieldName string, streamName ast.StreamName) {
	lname := strings.ToLower(fieldName)
	if fm, ok := f.content[lname]; ok {
		fm.add(streamName)
	} else {
		fm := newStreamFieldStore(f.isSchemaless, f.defaultStream)
		fm.add(streamName)
		f.content[lname] = fm
	}
}

func (f *fieldsMap) save(fieldName string, streamName ast.StreamName, field *ast.AliasRef) error {
	lname := strings.ToLower(fieldName)
	fm, ok := f.content[lname]
	if !ok {
		if streamName == ast.AliasStream || f.isSchemaless {
			fm = newStreamFieldStore(f.isSchemaless, f.defaultStream)
			f.content[lname] = fm
		} else {
			return fmt.Errorf("unknown field %s", fieldName)
		}
	}
	err := fm.ref(streamName, field)
	if err != nil {
		return fmt.Errorf("%s%s", err, fieldName)
	}
	return nil
}

func (f *fieldsMap) bind(fr *ast.FieldRef) error {
	lname := strings.ToLower(fr.Name)
	fm, ok := f.content[lname]
	if !ok {
		if f.isSchemaless && fr.Name != "" {
			fm = newStreamFieldStore(f.isSchemaless, f.defaultStream)
			f.content[lname] = fm
		} else {
			return fmt.Errorf("unknown field %s", fr.Name)
		}
	}
	err := fm.bindRef(fr)
	if err != nil {
		return fmt.Errorf("%s%s", err, fr.Name)
	}
	return nil
}

type streamFieldStore interface {
	add(k ast.StreamName)
	ref(k ast.StreamName, v *ast.AliasRef) error
	bindRef(f *ast.FieldRef) error
}

func newStreamFieldStore(isSchemaless bool, defaultStream ast.StreamName) streamFieldStore {
	if !isSchemaless {
		return &streamFieldMap{content: make(map[ast.StreamName]*ast.AliasRef)}
	} else {
		return &streamFieldMapSchemaless{content: make(map[ast.StreamName]*ast.AliasRef), defaultStream: defaultStream}
	}
}

type streamFieldMap struct {
	content map[ast.StreamName]*ast.AliasRef
}

// add the stream name must not be default.
// This is used when traversing stream schema
func (s *streamFieldMap) add(k ast.StreamName) {
	s.content[k] = nil
}

//bind for schema field, all keys must be created before running bind
// can bind alias & col. For alias, the stream name must be empty; For col, the field must be a col
func (s *streamFieldMap) ref(k ast.StreamName, v *ast.AliasRef) error {
	if k == ast.AliasStream { // must not exist, save alias ref for alias
		_, ok := s.content[k]
		if ok {
			return fmt.Errorf("duplicate alias ")
		}
		s.content[k] = v
	} else { // the key must exist after the schema travers, do validation
		if k == ast.DefaultStream { // In schema mode, default stream won't be a key
			l := len(s.content)
			if l == 0 {
				return fmt.Errorf("unknow field ")
			} else if l == 1 {
				// valid, do nothing
			} else {
				return fmt.Errorf("ambiguous field ")
			}
		} else {
			_, ok := s.content[k]
			if !ok {
				return fmt.Errorf("unknow field %s.", k)
			}
		}
	}
	return nil
}

func (s *streamFieldMap) bindRef(fr *ast.FieldRef) error {
	l := len(s.content)
	if fr.StreamName == "" {
		fr.StreamName = ast.DefaultStream
	}
	k := fr.StreamName
	if k == ast.DefaultStream {
		switch l {
		case 0:
			return fmt.Errorf("unknown field ")
		case 1: // if alias, return this
			for sk, sv := range s.content {
				fr.RefSelection(sv)
				fr.StreamName = sk
			}
			return nil
		default:
			r, ok := s.content[ast.AliasStream] // if alias exists
			if ok {
				fr.RefSelection(r)
				fr.StreamName = ast.AliasStream
				return nil
			} else {
				return fmt.Errorf("ambiguous field ")
			}
		}
	} else {
		r, ok := s.content[k]
		if ok {
			fr.RefSelection(r)
			return nil
		} else {
			return fmt.Errorf("unknown field %s.", k)
		}
	}
}

type streamFieldMapSchemaless struct {
	content       map[ast.StreamName]*ast.AliasRef
	defaultStream ast.StreamName
}

// add this should not be called for schemaless
func (s *streamFieldMapSchemaless) add(k ast.StreamName) {
	s.content[k] = nil
}

//bind for schemaless field, create column if not exist
// can bind alias & col. For alias, the stream name must be empty; For col, the field must be a col
func (s *streamFieldMapSchemaless) ref(k ast.StreamName, v *ast.AliasRef) error {
	if k == ast.AliasStream { // must not exist
		_, ok := s.content[k]
		if ok {
			return fmt.Errorf("duplicate alias ")
		}
		s.content[k] = v
	} else { // the key may or may not exist. But always have only one default stream field.
		// Replace with stream name if another stream found. The key can be duplicate
		l := len(s.content)
		if k == ast.DefaultStream { // In schemaless mode, default stream can only exist when length is 1
			if l < 1 {
				// valid, do nothing
			} else {
				return fmt.Errorf("ambiguous field ")
			}
		} else {
			if l == 1 {
				for sk := range s.content {
					if sk == ast.DefaultStream {
						delete(s.content, k)
					}
				}
			}
		}
	}
	return nil
}

func (s *streamFieldMapSchemaless) bindRef(fr *ast.FieldRef) error {
	l := len(s.content)
	if fr.StreamName == "" || fr.StreamName == ast.DefaultStream {
		if l == 1 {
			for sk := range s.content {
				fr.StreamName = sk
			}
		}
	}
	k := fr.StreamName
	if k == ast.DefaultStream {
		switch l {
		case 0: // must be a column because alias are fields and have been traversed
			// reserve a hole and do nothing
			fr.StreamName = s.defaultStream
			s.content[s.defaultStream] = nil
			return nil
		case 1: // if alias or single col, return this
			for sk, sv := range s.content {
				fr.RefSelection(sv)
				fr.StreamName = sk
			}
			return nil
		default:
			r, ok := s.content[ast.AliasStream] // if alias exists
			if ok {
				fr.RefSelection(r)
				fr.StreamName = ast.AliasStream
				return nil
			} else {
				fr.StreamName = s.defaultStream
			}
		}
	}

	if fr.StreamName != ast.DefaultStream {
		r, ok := s.content[k]
		if !ok { // reserver a hole
			s.content[k] = nil
		} else {
			fr.RefSelection(r)
		}
		return nil
	}
	return fmt.Errorf("ambiguous field ")
}

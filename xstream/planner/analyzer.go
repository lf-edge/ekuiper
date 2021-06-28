package planner

import (
	"fmt"
	"github.com/emqx/kuiper/common/kv"
	"github.com/emqx/kuiper/xsql"
	"strconv"
	"strings"
)

// Analyze the select statement by decorating the info from stream statement.
// Typically, set the correct stream name for fieldRefs
func decorateStmt(s *xsql.SelectStatement, store kv.KeyValue) ([]*xsql.StreamStmt, error) {
	streamsFromStmt := xsql.GetStreams(s)
	streamStmts := make([]*xsql.StreamStmt, len(streamsFromStmt))
	isSchemaless := false
	for i, s := range streamsFromStmt {
		streamStmt, err := xsql.GetDataSource(store, s)
		if err != nil {
			return nil, fmt.Errorf("fail to get stream %s, please check if stream is created", s)
		}
		streamStmts[i] = streamStmt
		// TODO fine grain control of schemaless
		if streamStmt.StreamFields == nil {
			isSchemaless = true
		}
	}

	dsn := xsql.DefaultStream
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
		walkErr     error
		aliasFields []*xsql.Field
	)
	// Scan columns fields: bind all field refs, collect alias
	for i, f := range s.Fields {
		xsql.WalkFunc(f.Expr, func(n xsql.Node) bool {
			switch f := n.(type) {
			case *xsql.FieldRef:
				if f.IsSQLField() {
					walkErr = fieldsMap.bind(f)
				}
			}
			return true
		})
		if walkErr != nil {
			return nil, walkErr
		}
		// assign name for anonymous select expression
		if f.Name == "" && f.AName == "" {
			s.Fields[i].Name = fieldsMap.getDefaultName()
		}
		if f.AName != "" {
			aliasFields = append(aliasFields, &s.Fields[i])
		}
	}
	// bind alias field expressions
	for _, f := range aliasFields {
		ar, err := xsql.NewAliasRef(f.Expr)
		if err != nil {
			walkErr = err
		} else {
			f.Expr = &xsql.FieldRef{
				StreamName: xsql.AliasStream,
				Name:       f.AName,
				AliasRef:   ar,
			}
			walkErr = fieldsMap.save(f.AName, xsql.AliasStream, ar)
		}
	}
	// bind field ref for alias AND set StreamName for all field ref
	xsql.WalkFunc(s, func(n xsql.Node) bool {
		switch f := n.(type) {
		case xsql.Fields: // do not bind selection fields, should have done above
			return false
		case *xsql.FieldRef:
			walkErr = fieldsMap.bind(f)
		}
		return true
	})
	if walkErr != nil {
		return nil, walkErr
	}
	walkErr = validate(s)
	return streamStmts, walkErr
}

func validate(s *xsql.SelectStatement) (err error) {
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
	xsql.WalkFunc(s, func(n xsql.Node) bool {
		switch f := n.(type) {
		case *xsql.Call:
			// aggregate call should not have any aggregate arg
			if xsql.IsAggFunc(f) {
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
func allAggregate(expr xsql.Expr) (r bool) {
	r = true
	xsql.WalkFunc(expr, func(n xsql.Node) bool {
		switch f := expr.(type) {
		case *xsql.BinaryExpr:
			switch f.OP {
			case xsql.SUBSET, xsql.ARROW:
				// do nothing
			default:
				r = allAggregate(f.LHS) && allAggregate(f.RHS)
				return false
			}
		case *xsql.Call, *xsql.FieldRef:
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
	defaultStream xsql.StreamName
}

func newFieldsMap(isSchemaless bool, defaultStream xsql.StreamName) *fieldsMap {
	return &fieldsMap{content: make(map[string]streamFieldStore), isSchemaless: isSchemaless, defaultStream: defaultStream}
}

func (f *fieldsMap) reserve(fieldName string, streamName xsql.StreamName) {
	if fm, ok := f.content[strings.ToLower(fieldName)]; ok {
		fm.add(streamName)
	} else {
		fm := newStreamFieldStore(f.isSchemaless, f.defaultStream)
		fm.add(streamName)
		f.content[strings.ToLower(fieldName)] = fm
	}
}

func (f *fieldsMap) save(fieldName string, streamName xsql.StreamName, field *xsql.AliasRef) error {
	fm, ok := f.content[strings.ToLower(fieldName)]
	if !ok {
		if streamName == xsql.AliasStream || f.isSchemaless {
			fm = newStreamFieldStore(f.isSchemaless, f.defaultStream)
			f.content[strings.ToLower(fieldName)] = fm
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

func (f *fieldsMap) bind(fr *xsql.FieldRef) error {
	fm, ok := f.content[strings.ToLower(fr.Name)]
	if !ok {
		if f.isSchemaless && fr.Name != "" {
			fm = newStreamFieldStore(f.isSchemaless, f.defaultStream)
			f.content[strings.ToLower(fr.Name)] = fm
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

func (f *fieldsMap) getDefaultName() string {
	for i := 0; i < 2048; i++ {
		key := xsql.DEFAULT_FIELD_NAME_PREFIX + strconv.Itoa(i)
		if _, ok := f.content[key]; !ok {
			return key
		}
	}
	return ""
}

type streamFieldStore interface {
	add(k xsql.StreamName)
	ref(k xsql.StreamName, v *xsql.AliasRef) error
	bindRef(f *xsql.FieldRef) error
}

func newStreamFieldStore(isSchemaless bool, defaultStream xsql.StreamName) streamFieldStore {
	if !isSchemaless {
		return &streamFieldMap{content: make(map[xsql.StreamName]*xsql.AliasRef)}
	} else {
		return &streamFieldMapSchemaless{content: make(map[xsql.StreamName]*xsql.AliasRef), defaultStream: defaultStream}
	}
}

type streamFieldMap struct {
	content map[xsql.StreamName]*xsql.AliasRef
}

// add the stream name must not be default.
// This is used when traversing stream schema
func (s *streamFieldMap) add(k xsql.StreamName) {
	s.content[k] = nil
}

//bind for schema field, all keys must be created before running bind
// can bind alias & col. For alias, the stream name must be empty; For col, the field must be a col
func (s *streamFieldMap) ref(k xsql.StreamName, v *xsql.AliasRef) error {
	if k == xsql.AliasStream { // must not exist, save alias ref for alias
		_, ok := s.content[k]
		if ok {
			return fmt.Errorf("duplicate alias ")
		}
		s.content[k] = v
	} else { // the key must exist after the schema travers, do validation
		if k == xsql.DefaultStream { // In schema mode, default stream won't be a key
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

func (s *streamFieldMap) bindRef(fr *xsql.FieldRef) error {
	l := len(s.content)
	if fr.StreamName == "" {
		fr.StreamName = xsql.DefaultStream
	}
	k := fr.StreamName
	if k == xsql.DefaultStream {
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
			r, ok := s.content[xsql.AliasStream] // if alias exists
			if ok {
				fr.RefSelection(r)
				fr.StreamName = xsql.AliasStream
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
	content       map[xsql.StreamName]*xsql.AliasRef
	defaultStream xsql.StreamName
}

// add this should not be called for schemaless
func (s *streamFieldMapSchemaless) add(k xsql.StreamName) {
	s.content[k] = nil
}

//bind for schemaless field, create column if not exist
// can bind alias & col. For alias, the stream name must be empty; For col, the field must be a col
func (s *streamFieldMapSchemaless) ref(k xsql.StreamName, v *xsql.AliasRef) error {
	if k == xsql.AliasStream { // must not exist
		_, ok := s.content[k]
		if ok {
			return fmt.Errorf("duplicate alias ")
		}
		s.content[k] = v
	} else { // the key may or may not exist. But always have only one default stream field.
		// Replace with stream name if another stream found. The key can be duplicate
		l := len(s.content)
		if k == xsql.DefaultStream { // In schemaless mode, default stream can only exist when length is 1
			if l < 1 {
				// valid, do nothing
			} else {
				return fmt.Errorf("ambiguous field ")
			}
		} else {
			if l == 1 {
				for sk := range s.content {
					if sk == xsql.DefaultStream {
						delete(s.content, k)
					}
				}
			}
		}
	}
	return nil
}

func (s *streamFieldMapSchemaless) bindRef(fr *xsql.FieldRef) error {
	l := len(s.content)
	if fr.StreamName == "" || fr.StreamName == xsql.DefaultStream {
		if l == 1 {
			for sk := range s.content {
				fr.StreamName = sk
			}
		} else {
			fr.StreamName = s.defaultStream
		}
	}
	k := fr.StreamName
	if k == xsql.DefaultStream {
		switch l {
		case 0: // must be a column because alias are fields and have been traversed
			// reserve a hole and do nothing
			s.content[k] = nil
			return nil
		case 1: // if alias or single col, return this
			for sk, sv := range s.content {
				fr.RefSelection(sv)
				fr.StreamName = sk
			}
			return nil
		default:
			r, ok := s.content[xsql.AliasStream] // if alias exists
			if ok {
				fr.RefSelection(r)
				fr.StreamName = xsql.AliasStream
				return nil
			} else {
				return fmt.Errorf("ambiguous field ")
			}
		}
	} else {
		r, ok := s.content[k]
		if !ok { // reserver a hole
			s.content[k] = nil
		} else {
			fr.RefSelection(r)
		}
		return nil
	}
}

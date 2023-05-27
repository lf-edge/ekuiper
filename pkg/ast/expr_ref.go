// Copyright 2023 EMQ Technologies Co., Ltd.
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

package ast

import (
	"fmt"
	"regexp"
	"strings"
)

type LikePattern struct {
	Expr    Expr
	Pattern *regexp.Regexp
}

func (l *LikePattern) expr() {}
func (l *LikePattern) node() {}
func (l *LikePattern) String() string {
	return "likePattern:" + l.Pattern.String()
}

func (l *LikePattern) Compile(likestr string) (*regexp.Regexp, error) {
	regstr := strings.ReplaceAll(strings.NewReplacer(
		`\%`, `\%`,
		`\_`, `\_`,
		`%`, `.*`,
		`_`, `.`,
	).Replace(likestr), `\`, `\\`)
	re, err := regexp.Compile("^" + regstr + "$")
	if err != nil {
		return nil, err
	}
	return re, nil
}

// FieldRef could be
//  1. SQL Field
//     1.1 Explicit field "stream.col"
//     1.2 Implicit field "col"  -> only exist in schemaless stream. Otherwise, explicit stream name will be bound
//     1.3 Alias field "expr as c" -> refer to an Expression or column
type FieldRef struct {
	// optional, bind in analyzer, empty means alias, default means not set
	// MUST have after binding for SQL fields. For 1.2,1.3 and 1.4, use special constant as stream name
	StreamName StreamName
	// optional, set only once. For selections, empty name will be assigned a default name
	// MUST have after binding, assign a name for 1.4
	Name string
	// Only for alias
	*AliasRef
}

func (fr *FieldRef) expr() {}
func (fr *FieldRef) node() {}
func (fr *FieldRef) IsColumn() bool {
	return fr.StreamName != AliasStream && fr.StreamName != ""
}

func (fr *FieldRef) String() string {
	sn := ""
	n := ""
	if fr.StreamName != "" {
		sn += "streamName:" + string(fr.StreamName)
	}
	if fr.Name != "" {
		if fr.StreamName != "" {
			n += ", "
		}
		n += "fieldName:" + fr.Name
	}
	return "fieldRef:{ " + sn + n + " }"
}

func (fr *FieldRef) IsAlias() bool {
	return fr.StreamName == AliasStream
}

func (fr *FieldRef) RefSelection(a *AliasRef) {
	fr.AliasRef = a
}

// RefSources Must call after binding or will get empty
func (fr *FieldRef) RefSources() []StreamName {
	if fr.StreamName == AliasStream {
		return fr.refSources
	} else if fr.StreamName != "" {
		return []StreamName{fr.StreamName}
	} else {
		return nil
	}
}

// SetRefSource Only call this for alias field ref
func (fr *FieldRef) SetRefSource(names []StreamName) {
	fr.refSources = names
}

type AliasRef struct {
	// MUST have, It is used for evaluation
	Expression Expr
	// MUST have after binding, calculate once in initializer. Could be 0 when alias an Expression without col like "1+2"
	refSources []StreamName
	// optional, lazy set when calculating isAggregate
	IsAggregate *bool
}

func NewAliasRef(e Expr) (*AliasRef, error) {
	r := make(map[StreamName]bool)
	var walkErr error
	WalkFunc(e, func(n Node) bool {
		switch f := n.(type) {
		case *FieldRef:
			switch f.StreamName {
			case AliasStream:
				walkErr = fmt.Errorf("cannot use alias %s inside another alias %v", f.Name, e)
				return false
			default:
				r[f.StreamName] = true
			}
		}
		return true
	})
	if walkErr != nil {
		return nil, walkErr
	}
	rs := make([]StreamName, 0)
	for k := range r {
		rs = append(rs, k)
	}
	return &AliasRef{
		Expression: e,
		refSources: rs,
	}, nil
}

// MockAliasRef is for testing only.
func MockAliasRef(e Expr, r []StreamName, a *bool) *AliasRef {
	return &AliasRef{e, r, a}
}

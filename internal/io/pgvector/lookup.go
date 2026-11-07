// Copyright 2026 EMQ Technologies Co., Ltd.
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

// Package pgvector provides a lookup source backed by a PostgreSQL table
// with a pgvector column.  Similarity search is expressed with the pgvector
// distance operators:
//
//	COSINE -> ORDER BY embedding <=> $query LIMIT topK
//	L2     -> ORDER BY embedding <-> $query LIMIT topK
//	IP     -> ORDER BY embedding <#> $query LIMIT topK
//
// Configuration:
//
//	"pgvector": {
//	  "url":          "postgres://user:pass@127.0.0.1:5432/mydb", // DSN
//	  "table":        "fault_history",                            // table to search
//	  "vectorField":  "embedding",                                // pgvector column
//	  "dimension":    128,                                        // optional, enforced when > 0
//	  "topK":         5,                                          // number of results
//	  "metric":       "COSINE"                                    // COSINE | L2 | IP
//	}
//
// Lookup usage keeps the equality form required by the SQL planner; the join
// key on the lookup side must be the configured vectorField:
//
//	SELECT s.alarm, h.sop
//	FROM alarm_stream s
//	  LOOKUP JOIN fault_history h ON s.query_vec = h.embedding
package pgvector

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	_ "github.com/lib/pq"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/vector"
)

type conf struct {
	URL         string `json:"url"`
	Table       string `json:"table"`
	VectorField string `json:"vectorField"`
	Dimension   int    `json:"dimension"`
	TopK        int    `json:"topK"`
	Metric      string `json:"metric"`
	Timeout     int    `json:"timeout"`
}

type lookupSource struct {
	c      *conf
	metric vector.Metric
	db     *sql.DB
	dsn    string
	op     string // pgvector distance operator for the configured metric
}

const (
	defaultTopK   = 5
	defaultTimout = 30
)

func metricOperator(m vector.Metric) (string, error) {
	switch m {
	case vector.MetricCosine:
		return "<=>", nil // cosine distance
	case vector.MetricL2:
		return "<->", nil // euclidean distance
	case vector.MetricIP:
		return "<#>", nil // negative inner product
	default:
		return "", fmt.Errorf("unsupported metric %s", m)
	}
}

func (s *lookupSource) Provision(ctx api.StreamContext, props map[string]any) error {
	return s.Validate(props)
}

func (s *lookupSource) Validate(props map[string]any) error {
	cfg := &conf{}
	if err := cast.MapToStruct(props, cfg); err != nil {
		return err
	}
	if cfg.URL == "" {
		return errors.New("pgvector url is null")
	}
	if cfg.Table == "" {
		return errors.New("pgvector table is null")
	}
	// Basic identifier sanity check to keep the generated SQL safe.
	if !validIdentifier(cfg.Table) {
		return fmt.Errorf("pgvector table %q is not a valid identifier", cfg.Table)
	}
	if cfg.VectorField == "" {
		return errors.New("pgvector vectorField is null")
	}
	if !validIdentifier(cfg.VectorField) {
		return fmt.Errorf("pgvector vectorField %q is not a valid identifier", cfg.VectorField)
	}
	if cfg.TopK <= 0 {
		cfg.TopK = defaultTopK
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimout
	}
	m, err := vector.ParseMetric(cfg.Metric)
	if err != nil {
		return err
	}
	op, err := metricOperator(m)
	if err != nil {
		return err
	}
	s.c = cfg
	s.metric = m
	s.dsn = cfg.URL
	s.op = op
	return nil
}

func validIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if r == '_' || r == '.' || r == '$' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9' && i > 0) {
			continue
		}
		return false
	}
	return true
}

func (s *lookupSource) Ping(ctx api.StreamContext, props map[string]any) error {
	if err := s.Validate(props); err != nil {
		return err
	}
	db, err := s.open()
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Ping()
}

func (s *lookupSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	db, err := s.open()
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	if err := db.Ping(); err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (s *lookupSource) open() (*sql.DB, error) {
	if s.db != nil {
		return s.db, nil
	}
	db, err := sql.Open("postgres", s.dsn)
	if err != nil {
		return nil, err
	}
	if s.c.Timeout > 0 {
		timeout := time.Duration(s.c.Timeout) * time.Second
		db.SetConnMaxLifetime(timeout)
		db.SetConnMaxIdleTime(timeout)
	}
	s.db = db
	return db, nil
}

func (s *lookupSource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Opening pgvector lookup source: table=%s field=%s topK=%d metric=%s",
		s.c.Table, s.c.VectorField, s.c.TopK, s.metric)
	return nil
}

func (s *lookupSource) Close(ctx api.StreamContext) error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Lookup runs a pgvector similarity search.  The first key/value pair
// carries the query vector for the configured vectorField; every additional
// pair is applied as an equality filter.
func (s *lookupSource) Lookup(ctx api.StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error) {
	ctx.GetLogger().Debugf("Lookup pgvector %v", keys)
	if len(keys) == 0 || len(keys) != len(values) {
		return nil, fmt.Errorf("pgvector lookup expects matched keys and values, got keys %v values %v", keys, values)
	}

	queryVec, filters, filterValues, err := s.extractQuery(keys, values)
	if err != nil {
		return nil, err
	}
	if err := vector.ValidateDimension(queryVec, s.c.Dimension); err != nil {
		return nil, err
	}

	query, args := s.buildQuery(fields, queryVec, filters, filterValues)
	ctx.GetLogger().Debugf("pgvector query: %s", query)

	db, err := s.open()
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("pgvector query failed: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	results := make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// extractQuery picks the query vector out of the lookup pairs and collects
// the remaining pairs as equality filters.
func (s *lookupSource) extractQuery(keys []string, values []any) ([]float64, []string, []any, error) {
	var queryVec []float64
	var filters []string
	var filterValues []any
	for i, k := range keys {
		if k == s.c.VectorField && queryVec == nil {
			v, err := vector.ParseVector(values[i])
			if err != nil {
				return nil, nil, nil, fmt.Errorf("parse query vector for field %s: %v", k, err)
			}
			queryVec = v
			continue
		}
		filters = append(filters, fmt.Sprintf("%s = $%d", k, len(filters)+2))
		filterValues = append(filterValues, values[i])
	}
	if queryVec == nil {
		return nil, nil, nil, fmt.Errorf("no query vector found: expected an equality on the configured vectorField %s", s.c.VectorField)
	}
	return queryVec, filters, filterValues, nil
}

// buildQuery assembles the parameterized similarity search.  Parameter 1 is
// always the query vector; filter parameters start at 2.
func (s *lookupSource) buildQuery(fields []string, queryVec []float64, filters []string, filterValues []any) (string, []any) {
	cols := "*"
	if len(fields) > 0 {
		quoted := make([]string, 0, len(fields))
		for _, f := range fields {
			if f == s.c.VectorField || validIdentifier(f) {
				quoted = append(quoted, f)
			} else {
				quoted = append(quoted, `"`+strings.ReplaceAll(f, `"`, ``)+`"`)
			}
		}
		cols = strings.Join(quoted, ", ")
	}
	sb := strings.Builder{}
	sb.WriteString("SELECT ")
	sb.WriteString(cols)
	sb.WriteString(" FROM ")
	sb.WriteString(s.c.Table)
	if len(filters) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(filters, " AND "))
	}
	sb.WriteString(" ORDER BY ")
	sb.WriteString(s.c.VectorField)
	sb.WriteString(" ")
	sb.WriteString(s.op)
	sb.WriteString(" $1::vector LIMIT ")
	sb.WriteString(fmt.Sprintf("%d", s.c.TopK))
	args := make([]any, 0, len(filterValues)+1)
	args = append(args, vector.FormatVector(queryVec))
	args = append(args, filterValues...)
	return sb.String(), args
}

func GetLookupSource() api.Source {
	return &lookupSource{}
}

var (
	_ api.LookupSource  = &lookupSource{}
	_ util.PingableConn = &lookupSource{}
)

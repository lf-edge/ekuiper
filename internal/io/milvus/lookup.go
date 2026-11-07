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

// Package milvus provides a lookup source backed by a Milvus collection.
// It performs ANN similarity search through the Milvus v2 RESTful API, so no
// extra SDK dependency is required.
//
// Configuration:
//
//	"milvus": {
//	  "endpoint":     "http://127.0.0.1:19530", // Milvus RESTful server
//	  "collection":   "fault_history",          // collection to search
//	  "vectorField":  "embedding",              // ANN search field
//	  "dimension":    128,                      // optional, enforced when > 0
//	  "topK":         5,                        // number of results to return
//	  "metric":       "COSINE",                 // COSINE | L2 | IP
//	  "token":        "",                       // optional bearer token / api key
//	  "database":     ""                        // optional database name
//	}
//
// Lookup usage keeps the equality form required by the SQL planner; the join
// key on the lookup side must be the configured vectorField:
//
//	SELECT s.alarm, h.sop
//	FROM alarm_stream s
//	  LOOKUP JOIN fault_history v ON s.query_vec = v.embedding
package milvus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/vector"
)

type conf struct {
	Endpoint    string `json:"endpoint"`
	Collection  string `json:"collection"`
	VectorField string `json:"vectorField"`
	Dimension   int    `json:"dimension"`
	TopK        int    `json:"topK"`
	Metric      string `json:"metric"`
	Token       string `json:"token"`
	Database    string `json:"database"`
	Timeout     int    `json:"timeout"`
}

type lookupSource struct {
	c        *conf
	metric   vector.Metric
	client   *http.Client
	endpoint string
}

const (
	searchPath    = "/v2/vectordb/entities/search"
	hasCollection = "/v2/vectordb/collections/has"
	defaultTopK   = 5
	defaultTimout = 30
)

func (s *lookupSource) Provision(ctx api.StreamContext, props map[string]any) error {
	return s.Validate(props)
}

func (s *lookupSource) Validate(props map[string]any) error {
	cfg := &conf{}
	if err := cast.MapToStruct(props, cfg); err != nil {
		return err
	}
	if cfg.Endpoint == "" {
		return errors.New("milvus endpoint is null")
	}
	if _, err := url.Parse(cfg.Endpoint); err != nil {
		return fmt.Errorf("milvus endpoint %s is invalid: %v", cfg.Endpoint, err)
	}
	if cfg.Collection == "" {
		return errors.New("milvus collection is null")
	}
	if cfg.VectorField == "" {
		return errors.New("milvus vectorField is null")
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
	s.c = cfg
	s.metric = m
	u, err := url.Parse(strings.TrimRight(cfg.Endpoint, "/"))
	if err != nil {
		return err
	}
	s.endpoint = u.String()
	return nil
}

func (s *lookupSource) Ping(ctx api.StreamContext, props map[string]any) error {
	if err := s.Validate(props); err != nil {
		return err
	}
	payload := map[string]any{"collectionName": s.c.Collection}
	if s.c.Database != "" {
		payload["dbName"] = s.c.Database
	}
	var out struct {
		Code    int `json:"code"`
		Message string
		Has     bool `json:"has"`
	}
	err := s.post(ctx, hasCollection, payload, &out)
	if err != nil {
		return err
	}
	if out.Code != 0 {
		return fmt.Errorf("milvus ping error(%d): %s", out.Code, out.Message)
	}
	if !out.Has {
		return fmt.Errorf("milvus collection %s does not exist", s.c.Collection)
	}
	return nil
}

func (s *lookupSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	if err := s.Ping(ctx, map[string]any{
		"endpoint": s.c.Endpoint, "collection": s.c.Collection,
		"database": s.c.Database,
	}); err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (s *lookupSource) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Opening milvus lookup source: %s/%s (field=%s, topK=%d, metric=%s)",
		s.c.Endpoint, s.c.Collection, s.c.VectorField, s.c.TopK, s.metric)
	return nil
}

func (s *lookupSource) Close(ctx api.StreamContext) error {
	return nil
}

// Lookup runs a similarity search.  The first key/value pair carries the
// query vector for the configured vectorField; every additional pair is
// applied as an equality filter expression.
func (s *lookupSource) Lookup(ctx api.StreamContext, fields []string, keys []string, values []any) ([]map[string]any, error) {
	ctx.GetLogger().Debugf("Lookup milvus %v", keys)
	if len(keys) == 0 || len(keys) != len(values) {
		return nil, fmt.Errorf("milvus lookup expects matched keys and values, got keys %v values %v", keys, values)
	}

	queryVec, filters, err := s.extractQuery(keys, values)
	if err != nil {
		return nil, err
	}
	if err := vector.ValidateDimension(queryVec, s.c.Dimension); err != nil {
		return nil, err
	}

	payload := map[string]any{
		"collectionName": s.c.Collection,
		"searchParams": map[string]any{
			"annsField":  s.c.VectorField,
			"topK":       s.c.TopK,
			"metricType": string(s.metric),
		},
		"data": []map[string]any{{"vector": queryVec}},
	}
	if s.c.Database != "" {
		payload["dbName"] = s.c.Database
	}
	if len(fields) > 0 {
		payload["outputFields"] = fields
	}
	if len(filters) > 0 {
		payload["filter"] = strings.Join(filters, " and ")
	}

	var out struct {
		Code    int              `json:"code"`
		Message string           `json:"message"`
		Data    []map[string]any `json:"data"`
	}
	if err := s.post(ctx, searchPath, payload, &out); err != nil {
		return nil, err
	}
	if out.Code != 0 {
		return nil, fmt.Errorf("milvus search error(%d): %s", out.Code, out.Message)
	}

	results := make([]map[string]any, 0, len(out.Data))
	for _, row := range out.Data {
		item := make(map[string]any, len(row))
		for k, v := range row {
			if k == "distance" || k == "score" {
				continue
			}
			item[k] = v
		}
		d := 0.0
		if dv, ok := row["distance"]; ok {
			if df, ok := toFloat(dv); ok {
				d = df
			}
		}
		item["distance"] = d
		item["score"] = vector.Score(s.metric, d)
		results = append(results, item)
	}
	return results, nil
}

// extractQuery picks the query vector out of the lookup pairs and turns the
// remaining pairs into Milvus equality filter expressions.
func (s *lookupSource) extractQuery(keys []string, values []any) ([]float64, []string, error) {
	var queryVec []float64
	filters := make([]string, 0, len(keys))
	for i, k := range keys {
		if k == s.c.VectorField && queryVec == nil {
			v, err := vector.ParseVector(values[i])
			if err != nil {
				return nil, nil, fmt.Errorf("parse query vector for field %s: %v", k, err)
			}
			queryVec = v
			continue
		}
		switch vv := values[i].(type) {
		case string:
			filters = append(filters, fmt.Sprintf("%s == %q", k, vv))
		default:
			filters = append(filters, fmt.Sprintf("%s == %v", k, vv))
		}
	}
	if queryVec == nil {
		return nil, nil, fmt.Errorf("no query vector found: expected an equality on the configured vectorField %s", s.c.VectorField)
	}
	return queryVec, filters, nil
}

func (s *lookupSource) post(ctx api.StreamContext, path string, payload any, out any) error {
	endpoint := s.endpoint + path
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(payload); err != nil {
		return err
	}
	timeout := time.Duration(s.c.Timeout) * time.Second
	var req *http.Request
	var err error
	if ctx != nil {
		req, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	} else {
		req, err = http.NewRequest(http.MethodPost, endpoint, &body)
	}
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.c.Token)
	}
	if s.client == nil {
		s.client = &http.Client{Timeout: timeout}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("milvus request %s failed: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		r, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("milvus request %s returned status %d: %s", path, resp.StatusCode, string(r))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func toFloat(v any) (float64, bool) {
	switch f := v.(type) {
	case float64:
		return f, true
	case float32:
		return float64(f), true
	case int:
		return float64(f), true
	case int64:
		return float64(f), true
	case string:
		d, err := strconv.ParseFloat(f, 64)
		if err != nil {
			return 0, false
		}
		return d, true
	default:
		return 0, false
	}
}

func GetLookupSource() api.Source {
	return &lookupSource{}
}

var (
	_ api.LookupSource  = &lookupSource{}
	_ util.PingableConn = &lookupSource{}
)

// Copyright 2024-2024 EMQ Technologies Co., Ltd.
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

//go:build trace || !core

package tracer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
)

type SpanExporter struct {
	remoteSpanExport *otlptrace.Exporter
	spanStorage      LocalSpanStorage
}

func NewSpanExporter(remoteCollector bool, remoteEndpoint string) (*SpanExporter, error) {
	s := &SpanExporter{}
	if remoteCollector {
		exporter, err := otlptracehttp.New(context.Background(),
			otlptracehttp.WithEndpoint(remoteEndpoint),
			otlptracehttp.WithInsecure(),
		)
		if err != nil {
			return nil, err
		}
		s.remoteSpanExport = exporter
	}
	if !conf.Config.OpenTelemetry.EnableLocalStorage {
		s.spanStorage = newLocalSpanMemoryStorage(conf.Config.OpenTelemetry.LocalTraceCapacity)
	} else {
		s.spanStorage = newSqlspanStorage()
	}
	return s, nil
}

func (l *SpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if l == nil {
		return nil
	}
	if l.remoteSpanExport != nil {
		err := l.remoteSpanExport.ExportSpans(ctx, spans)
		if err != nil {
			conf.Log.Warnf("export remote span err: %v", err)
		}
	}
	for _, span := range spans {
		if err := l.spanStorage.SaveSpan(span); err != nil {
			conf.Log.Errorf("save span err:%v", err)
		}
	}
	return nil
}

func (l *SpanExporter) Shutdown(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if l.remoteSpanExport != nil {
		err := l.remoteSpanExport.Shutdown(ctx)
		if err != nil {
			conf.Log.Warnf("shutdown remote span exporter err: %v", err)
		}
	}
	return nil
}

func (l *SpanExporter) GetTraceById(traceID string) (*LocalSpan, error) {
	return l.spanStorage.GetTraceById(traceID)
}

func (l *SpanExporter) GetTraceByRuleID(ruleID string, limit int64) ([]string, error) {
	return l.spanStorage.GetTraceByRuleID(ruleID, limit)
}

type LocalSpanStorage interface {
	SaveSpan(span sdktrace.ReadOnlySpan) error
	GetTraceById(traceID string) (*LocalSpan, error)
	GetTraceByRuleID(ruleID string, limit int64) ([]string, error)
}

type LocalSpanMemoryStorage struct {
	sync.RWMutex
	queue *Queue
	// traceid -> spanid -> span
	m map[string]map[string]*LocalSpan
	// rule -> traceID, traceIDs will have duplicates, need to dedup when return
	ruleTraces map[string][]string
}

func newLocalSpanMemoryStorage(capacity int) *LocalSpanMemoryStorage {
	return &LocalSpanMemoryStorage{
		queue:      NewQueue(capacity),
		ruleTraces: make(map[string][]string),
		m:          map[string]map[string]*LocalSpan{},
	}
}

func (l *LocalSpanMemoryStorage) SaveSpan(span sdktrace.ReadOnlySpan) error {
	l.Lock()
	defer l.Unlock()
	localSpan := FromReadonlySpan(span)
	return l.saveSpan(localSpan)
}

func (l *LocalSpanMemoryStorage) saveSpan(localSpan *LocalSpan) error {
	droppedTraceID := l.queue.Enqueue(localSpan)
	if droppedTraceID != "" {
		delete(l.m, droppedTraceID)
	}
	spanMap, ok := l.m[localSpan.TraceID]
	if !ok {
		spanMap = make(map[string]*LocalSpan)
		l.m[localSpan.TraceID] = spanMap
	}
	if len(localSpan.RuleID) > 0 {
		_, ok := l.ruleTraces[localSpan.RuleID]
		if !ok {
			l.ruleTraces[localSpan.RuleID] = make([]string, 0)
		}
		l.ruleTraces[localSpan.RuleID] = append(l.ruleTraces[localSpan.RuleID], localSpan.TraceID)
	}

	spanMap[localSpan.SpanID] = localSpan
	return nil
}

func (l *LocalSpanMemoryStorage) GetTraceById(traceID string) (*LocalSpan, error) {
	l.RLock()
	defer l.RUnlock()
	allSpans := l.m[traceID]
	if len(allSpans) < 1 {
		return nil, nil
	}
	rootSpan := findRootSpan(allSpans)
	if rootSpan == nil {
		return nil, nil
	}
	copySpan := make(map[string]*LocalSpan)
	for k, s := range allSpans {
		copySpan[k] = s
	}
	buildSpanLink(rootSpan, copySpan)
	return rootSpan, nil
}

func (l *LocalSpanMemoryStorage) GetTraceByRuleID(ruleID string, limit int64) ([]string, error) {
	l.RLock()
	defer l.RUnlock()
	allTraces := l.ruleTraces[ruleID]
	r := make([]string, 0)
	if limit < 1 {
		limit = int64(len(allTraces))
	}
	count := int64(0)
	traceMap := make(map[string]struct{})
	for i := len(allTraces) - 1; i >= 0; i-- {
		traceID := allTraces[i]
		if _, existed := traceMap[traceID]; existed {
			continue
		}
		traceMap[traceID] = struct{}{}
		r = append(r, traceID)
		count++
		if count >= limit {
			break
		}
	}
	return r, nil
}

func findRootSpan(allSpans map[string]*LocalSpan) *LocalSpan {
	for id1, span1 := range allSpans {
		if span1.ParentSpanID == "" {
			return span1
		}
		isRoot := true
		for id2, span2 := range allSpans {
			if id1 == id2 {
				continue
			}
			if span1.ParentSpanID == span2.SpanID {
				isRoot = false
				break
			}
		}
		if isRoot {
			return span1
		}
	}
	return nil
}

func buildSpanLink(cur *LocalSpan, OtherSpans map[string]*LocalSpan) {
	// should only build once?
	if len(cur.ChildSpan) > 0 {
		return
	}
	for k, otherSpan := range OtherSpans {
		if cur.SpanID == otherSpan.ParentSpanID {
			cur.ChildSpan = append(cur.ChildSpan, otherSpan)
			delete(OtherSpans, k)
		}
	}
	for _, span := range cur.ChildSpan {
		buildSpanLink(span, OtherSpans)
	}
}

// Queue is traceID FIFO queue with sized capacity
type Queue struct {
	m        map[string]struct{}
	items    []string
	capacity int
}

func NewQueue(capacity int) *Queue {
	return &Queue{
		m:        make(map[string]struct{}),
		items:    make([]string, 0),
		capacity: capacity,
	}
}

func (q *Queue) Enqueue(item *LocalSpan) string {
	_, ok := q.m[item.TraceID]
	if ok {
		return ""
	}
	dropped := ""
	if len(q.items) >= q.capacity {
		dropped = q.Dequeue()
	}
	q.items = append(q.items, item.TraceID)
	return dropped
}

func (q *Queue) Dequeue() string {
	if len(q.items) == 0 {
		return ""
	}
	traceID := q.items[0]
	q.items = q.items[1:]
	delete(q.m, traceID)
	return traceID
}

func (q *Queue) Len() int {
	return len(q.items)
}

type sqlSpanStorage struct{}

func newSqlspanStorage() *sqlSpanStorage {
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			if err := gcSqliteSpan(); err != nil {
				conf.Log.Warnf("gc sqlite trace span err:%v", err.Error())
			}
		}
	}()
	return &sqlSpanStorage{}
}

func (s *sqlSpanStorage) SaveSpan(span sdktrace.ReadOnlySpan) error {
	localSpan := FromReadonlySpan(span)
	return s.saveLocalSpan(localSpan)
}

func (s *sqlSpanStorage) GetTraceById(traceID string) (*LocalSpan, error) {
	return s.loadTraceByTraceID(traceID)
}

func (s *sqlSpanStorage) GetTraceByRuleID(ruleID string, limit int64) ([]string, error) {
	return s.loadTraceByRuleID(ruleID)
}

func (s *sqlSpanStorage) saveLocalSpan(span *LocalSpan) error {
	bs, err := span.ToBytes()
	if err != nil {
		return err
	}
	if store.TraceStores != nil {
		return store.TraceStores.Apply(func(db *sql.DB) error {
			stmt, err := db.Prepare("insert into trace(traceID , ruleID,value) values (?,?,?)")
			failpoint.Inject("injectTraceErr_1", func() {
				err = errors.New("injectTraceErr_1")
			})
			if err != nil {
				return err
			}
			_, err = stmt.Exec(span.TraceID, span.RuleID, bs)
			failpoint.Inject("injectTraceErr_2", func() {
				err = errors.New("injectTraceErr_2")
			})
			return err
		})
	}
	return nil
}

func (s *sqlSpanStorage) loadTraceByRuleID(ruleID string) ([]string, error) {
	traceIDList := make([]string, 0)
	err := store.TraceStores.Apply(func(db *sql.DB) error {
		stmt, err := db.Prepare("select traceID from trace where ruleID = ?")
		failpoint.Inject("injectTraceErr_3", func() {
			stmt.Close()
			err = errors.New("injectTraceErr_3")
		})
		if err != nil {
			return err
		}
		rows, err := stmt.Query(ruleID)
		failpoint.Inject("injectTraceErr_4", func() {
			rows.Close()
			stmt.Close()
			err = errors.New("injectTraceErr_4")
		})
		if err != nil {
			return err
		}
		var traceID string
		for rows.Next() {
			err := rows.Scan(&traceID)
			failpoint.Inject("injectTraceErr_5", func() {
				rows.Close()
				stmt.Close()
				err = errors.New("injectTraceErr_5")
			})
			if err != nil {
				return err
			}
			traceIDList = append(traceIDList, traceID)
		}
		return nil
	})
	return traceIDList, err
}

func (s *sqlSpanStorage) loadTraceByTraceID(traceID string) (*LocalSpan, error) {
	var valueList [][]byte
	err := store.TraceStores.Apply(func(db *sql.DB) error {
		stmt, err := db.Prepare("select value from trace where traceID = ?")
		failpoint.Inject("injectTraceErr_6", func() {
			stmt.Close()
			err = errors.New("injectTraceErr_6")
		})
		if err != nil {
			return err
		}
		rows, err := stmt.Query(traceID)
		failpoint.Inject("injectTraceErr_7", func() {
			rows.Close()
			stmt.Close()
			err = errors.New("injectTraceErr_7")
		})
		if err != nil {
			return err
		}
		var value []byte
		for rows.Next() {
			err := rows.Scan(&value)
			failpoint.Inject("injectTraceErr_8", func() {
				rows.Close()
				stmt.Close()
				err = errors.New("injectTraceErr_8")
			})
			if err != nil {
				return err
			}
			valueList = append(valueList, value)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	spans := make(map[string]*LocalSpan)
	for _, value := range valueList {
		l := &LocalSpan{}
		if err := json.Unmarshal(value, &l); err != nil {
			return nil, err
		}
		spans[l.SpanID] = l
	}
	rootSpan := findRootSpan(spans)
	if rootSpan == nil {
		return nil, nil
	}
	copySpan := make(map[string]*LocalSpan)
	for k, s := range spans {
		copySpan[k] = s
	}
	buildSpanLink(rootSpan, copySpan)
	return rootSpan, nil
}

func gcSqliteSpan() error {
	return store.TraceStores.Apply(func(db *sql.DB) error {
		_, err := db.Exec("DELETE FROM trace WHERE createdtimestamp < datetime('now', '-1 day')")
		return err
	})
}

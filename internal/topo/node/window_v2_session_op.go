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

package node

import (
	"fmt"
	"sort"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// SessionWindowOp runs a session window for each partition independently.
// Without a partition expression, all rows belong to the same partition.
//
// A session starts with the first row of its partition and is closed once no
// row of the partition arrives within the timeout. While the session keeps
// receiving rows, it is split at the aligned max duration checks, which happen
// every max duration as in the v1 session window.
//
// In processing time mode, the time advances with the incoming rows and a
// timer armed at the earliest pending timeout or check. In event time mode, it
// advances with the row timestamps and the watermarks.
type SessionWindowOp struct {
	*WindowV2Operator
	timeout       time.Duration
	maxDuration   time.Duration
	partitionExpr *ast.PartitionExpr
	isEventTime   bool
	state         *SessionWindowV2State
	// timer is only used in processing time mode. It is armed at timerAt,
	// which is never later than the earliest pending instant of all sessions.
	timer   *clock.Timer
	timerAt time.Time
}

// SessionWindowV2State is published once, mutated only by the operator
// goroutine, and frozen by the checkpoint barrier.
type SessionWindowV2State struct {
	Sessions map[string]*SessionWindowStatus
}

type SessionWindowStatus struct {
	// StartTime is the window start of the session: the timestamp of its first
	// row, or the latest max duration check that split it.
	StartTime time.Time
	// LastTime is the latest row timestamp. The session times out at
	// LastTime + timeout.
	LastTime time.Time
	// NextCheck is the next aligned max duration check of the session.
	NextCheck time.Time
	Scanner   *WindowScanner
}

type sessionWindowResult struct {
	end     time.Time
	results *xsql.WindowTuples
}

func NewSessionWindowOp(o *WindowV2Operator, isEventTime bool) (*SessionWindowOp, error) {
	if o.windowConfig.Length <= 0 || o.windowConfig.Interval <= 0 {
		return nil, fmt.Errorf("session window requires positive max duration and timeout, but got %v and %v", o.windowConfig.Length, o.windowConfig.Interval)
	}
	return &SessionWindowOp{
		WindowV2Operator: o,
		timeout:          o.windowConfig.Interval,
		maxDuration:      o.windowConfig.Length,
		partitionExpr:    o.windowConfig.PartitionExpr,
		isEventTime:      isEventTime,
		state: &SessionWindowV2State{
			Sessions: make(map[string]*SessionWindowStatus),
		},
	}, nil
}

func (s *SessionWindowOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := s.restoreState(ctx); err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	defer s.stopTimer()
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	handleItem := func(input any) {
		data, processed := s.ingest(ctx, input)
		if processed {
			return
		}
		switch row := data.(type) {
		case *xsql.WatermarkTuple:
			s.advanceAll(ctx, row.GetTimestamp())
		case *xsql.Tuple:
			s.onProcessStart(ctx, input)
			s.handleTuple(ctx, fv, row)
			s.onProcessEnd(ctx)
		}
	}
	for {
		// Due backlog rows and barriers ordered by the checkpoint handler
		// take priority over fresh input and the timer.
		if due, ok := s.nextDue(); ok {
			handleItem(due)
			continue
		}
		var timerC <-chan time.Time
		if s.timer != nil {
			timerC = s.timer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-timerC:
			s.timer = nil
			s.advanceAll(ctx, timex.GetNow())
		case input := <-s.input:
			handleItem(input)
		}
	}
}

func (s *SessionWindowOp) restoreState(ctx api.StreamContext) error {
	v, err := ctx.GetState(V2WindowInputsKey)
	if err != nil {
		return err
	}
	if v != nil {
		state, ok := v.(*SessionWindowV2State)
		if !ok {
			return fmt.Errorf("restore window V2 session state %T error, invalid type", v)
		}
		if state.Sessions == nil {
			state.Sessions = make(map[string]*SessionWindowStatus)
		}
		for _, status := range state.Sessions {
			if status.Scanner == nil {
				status.Scanner = &WindowScanner{Tuples: make([]*xsql.Tuple, 0)}
			}
		}
		s.state = state
	}
	if !s.isEventTime {
		// Close the sessions which expired during the downtime and arm the timer
		s.advanceAll(ctx, timex.GetNow())
	}
	return ctx.PutState(V2WindowInputsKey, s.state)
}

func (s *SessionWindowOp) handleTuple(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple) {
	key := calPartition(fv, s.partitionExpr, row)
	ts := row.Timestamp
	status, ok := s.state.Sessions[key]
	if ok {
		var results []sessionWindowResult
		closed := s.advance(status, ts, &results)
		s.send(ctx, results)
		if closed {
			delete(s.state.Sessions, key)
			ok = false
		}
	}
	if !ok {
		status = &SessionWindowStatus{
			StartTime: ts,
			LastTime:  ts,
			NextCheck: s.firstCheck(ts),
			Scanner:   &WindowScanner{Tuples: make([]*xsql.Tuple, 0)},
		}
		s.state.Sessions[key] = status
	}
	// Rows from a join may not be ordered, so the timeout never goes backward
	if ts.After(status.LastTime) {
		status.LastTime = ts
	}
	match, err := collectConditionMatch(fv, row, s.windowConfig.CollectCondition)
	if err != nil {
		s.onError(ctx, err)
	} else if match {
		status.Scanner.addTuple(row)
	}
	s.schedule(s.nextInstant(status))
}

// advanceAll moves all sessions to now, sends the windows in the order of
// their end time and arms the timer for the remaining sessions.
func (s *SessionWindowOp) advanceAll(ctx api.StreamContext, now time.Time) {
	keys := make([]string, 0, len(s.state.Sessions))
	for key := range s.state.Sessions {
		keys = append(keys, key)
	}
	// Sorted keys keep the order of the windows ending at the same time stable
	sort.Strings(keys)
	var results []sessionWindowResult
	for _, key := range keys {
		if s.advance(s.state.Sessions[key], now, &results) {
			delete(s.state.Sessions, key)
		}
	}
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].end.Before(results[j].end)
	})
	s.send(ctx, results)
	s.scheduleEarliest()
}

// advance moves the session forward to now. It splits the session at each
// passed max duration check and closes it once its timeout has passed. The
// windows to send are appended to results. It returns whether the session is
// closed.
func (s *SessionWindowOp) advance(status *SessionWindowStatus, now time.Time, results *[]sessionWindowResult) bool {
	for {
		deadline := status.LastTime.Add(s.timeout)
		// The timeout wins if it happens at the same time as the check
		if !deadline.After(now) && !deadline.After(status.NextCheck) {
			s.collectWindow(status, deadline, status.Scanner.Tuples, results)
			return true
		}
		check := status.NextCheck
		if check.After(now) {
			return false
		}
		status.NextCheck = check.Add(s.maxDuration)
		// Like the v1 session window, only split the session if its buffered
		// rows have lasted for the max duration
		if len(status.Scanner.Tuples) == 0 || check.Sub(status.Scanner.Tuples[0].Timestamp) < s.maxDuration {
			continue
		}
		emitted := make([]*xsql.Tuple, 0, len(status.Scanner.Tuples))
		rest := make([]*xsql.Tuple, 0)
		for _, tuple := range status.Scanner.Tuples {
			if tuple.Timestamp.Before(check) {
				emitted = append(emitted, tuple)
			} else {
				rest = append(rest, tuple)
			}
		}
		s.collectWindow(status, check, emitted, results)
		status.Scanner.Tuples = rest
		status.StartTime = check
	}
}

func (s *SessionWindowOp) collectWindow(status *SessionWindowStatus, end time.Time, tuples []*xsql.Tuple, results *[]sessionWindowResult) {
	// A session may have no rows left after a split or the collect filter
	if len(tuples) == 0 {
		return
	}
	wt := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0, len(tuples)),
	}
	for _, tuple := range tuples {
		wt.Content = append(wt.Content, tuple)
	}
	wt.WindowRange = xsql.NewWindowRange(status.StartTime.UnixMilli(), end.UnixMilli(), end.UnixMilli())
	*results = append(*results, sessionWindowResult{end: end, results: wt})
}

func (s *SessionWindowOp) send(ctx api.StreamContext, results []sessionWindowResult) {
	for _, r := range results {
		s.Broadcast(r.results)
		s.onSend(ctx, r.results)
	}
}

// firstCheck returns the first aligned max duration check after the session
// start. The checks happen every max duration like the v1 session window.
func (s *SessionWindowOp) firstCheck(start time.Time) time.Time {
	check := getAlignedWindowEndTime(start, s.windowConfig.RawInterval, s.windowConfig.TimeUnit)
	for !check.After(start) {
		check = check.Add(s.maxDuration)
	}
	return check
}

// nextInstant returns the earliest time when the session may change
func (s *SessionWindowOp) nextInstant(status *SessionWindowStatus) time.Time {
	deadline := status.LastTime.Add(s.timeout)
	if status.NextCheck.Before(deadline) {
		return status.NextCheck
	}
	return deadline
}

func (s *SessionWindowOp) scheduleEarliest() {
	var earliest time.Time
	for _, status := range s.state.Sessions {
		if next := s.nextInstant(status); earliest.IsZero() || next.Before(earliest) {
			earliest = next
		}
	}
	if !earliest.IsZero() {
		s.schedule(earliest)
	}
}

// schedule makes sure the timer fires no later than at. A timer firing
// earlier than needed does no harm because advancing to a time before the
// next instant of a session changes nothing. Event time mode is driven by the
// watermarks instead.
func (s *SessionWindowOp) schedule(at time.Time) {
	if s.isEventTime {
		return
	}
	if s.timer != nil {
		if !at.Before(s.timerAt) {
			return
		}
		s.timer.Stop()
	}
	s.timer = timex.GetTimerByTime(at)
	s.timerAt = at
}

func (s *SessionWindowOp) stopTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

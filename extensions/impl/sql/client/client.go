// Copyright 2024 EMQ Technologies Co., Ltd.
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

package client

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type SQLConnection struct {
	syncx.RWMutex
	url    string
	db     *sql.DB
	id     string
	closed bool
	// retireWG tracks handles replaced by Recover and closed
	// asynchronously. Add happens only under the write lock before
	// closed is set; Close waits after setting it — so no Add can
	// race the Wait. SQLConnection is always used by pointer.
	retireWG sync.WaitGroup
}

// SQLConnection is pool-recovered: runtime recovery belongs to the
// Pool worker, never to consumer retry loops.
var _ modules.PoolRecoverableConnection = (*SQLConnection)(nil)

// defaultAttemptTimeout bounds one Dial, Ping, or Recover attempt.
// Retry cadence and total retry lifetime are owned by the caller.
const defaultAttemptTimeout = 10 * time.Second

func (s *SQLConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	// dburl is canonical (url is only a compatibility alias): it wins when
	// both are present so the dialed database always matches the configured
	// dialect. See SQLConf.resolveDBURL, which applies the same precedence.
	// An explicitly empty dburl counts as absent, mirroring resolveDBURL's
	// len check, so Ping paths (which bypass resolveDBURL) accept
	// {dburl:"", url:valid}. A present but non-string value is a
	// misconfiguration and fails instead of silently falling back.
	if v, ok := props["dburl"]; ok && v != nil {
		if _, ok := v.(string); !ok {
			return fmt.Errorf("dburl should be defined as string")
		}
	}
	if v, ok := props["url"]; ok && v != nil {
		if _, ok := v.(string); !ok {
			return fmt.Errorf("url should be defined as string")
		}
	}
	dburlVal, _ := props["dburl"].(string)
	urlVal, _ := props["url"].(string)
	switch {
	case dburlVal != "":
		if urlVal != "" && urlVal != dburlVal {
			ctx.GetLogger().Warnf("both dburl and url are set with different values, using dburl")
		}
	case urlVal != "":
		dburlVal = urlVal
	default:
		return fmt.Errorf("dburl should be defined")
	}
	dburl := dburlVal
	ctx.GetLogger().Infof("create db with url:%v", dburl)

	s.url = dburl
	s.id = conId
	return nil
}

func (s *SQLConnection) GetId(ctx api.StreamContext) string {
	return s.id
}

func (s *SQLConnection) Dial(ctx api.StreamContext) error {
	s.Lock()
	defer s.Unlock()
	dialCtx, cancel := context.WithTimeout(ctx, defaultAttemptTimeout)
	defer cancel()
	return s.dial(dialCtx)
}

func (s *SQLConnection) GetDB() *sql.DB {
	s.RLock()
	defer s.RUnlock()
	return s.db
}

func (s *SQLConnection) Ping(ctx api.StreamContext) error {
	// Pure health check: a single bounded attempt, never a dial. An
	// absent handle means Dial never succeeded (or Close already ran);
	// creating the handle belongs to Dial/Recover, not to a status
	// read. Read-locked: Ping observes but never mutates.
	s.RLock()
	defer s.RUnlock()
	pingCtx, cancel := context.WithTimeout(ctx, defaultAttemptTimeout)
	defer cancel()
	if s.db == nil {
		return fmt.Errorf("sql connection %s has no database handle", s.id)
	}
	return s.db.PingContext(pingCtx)
}

func (s *SQLConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	// do nothing
}

func (s *SQLConnection) Close(ctx api.StreamContext) error {
	s.Lock()
	if s.closed {
		s.Unlock()
		return nil
	}
	ctx.GetLogger().Infof("close db with url:%v", s.url)
	if s.db != nil {
		_ = s.db.Close()
	}
	s.closed = true
	s.Unlock()
	// Drain handles retired by Recover: each retired Close runs
	// detached so a slow old pool never stalls the hot path, but the
	// logical Close still waits for all of them — no handle outlives
	// the connection. The retire goroutines never take this lock, so
	// waiting here cannot deadlock.
	s.retireWG.Wait()
	return nil
}

func CreateConnection(ctx api.StreamContext) modules.Connection {
	return &SQLConnection{}
}

func (s *SQLConnection) dial(ctx context.Context) error {
	db, err := openVerifiedDB(s.url, ctx)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

// Recover implements modules.PoolRecoverableConnection: one bounded
// attempt that builds a candidate, verifies it, and installs it. No
// internal retry or backoff (the Pool worker owns the rhythm); ctx
// carries the Pool's per-attempt deadline plus lifecycle
// cancellation. The replaced handle retires asynchronously — closed
// detached from this call so a slow old pool never stalls the hot
// path, drained by Close so nothing leaks past the logical lifetime.
// A failure leaves the previous handle untouched; the Pool worker
// keeps owning the episode (Ping verification, then retries with
// backoff until success).
func (s *SQLConnection) Recover(ctx api.StreamContext) error {
	recCtx, cancel := context.WithTimeout(ctx, defaultAttemptTimeout)
	defer cancel()
	db, err := openVerifiedDB(s.url, recCtx)
	if err != nil {
		return err
	}
	s.Lock()
	if s.closed {
		// Raced with the logical Close: dispose the candidate.
		// The Pool joins this worker before its own Close, so the
		// dispose here is the only Close the candidate gets.
		s.Unlock()
		_ = db.Close()
		return fmt.Errorf("sql connection %s closed during recovery", s.id)
	}
	old := s.db
	s.db = db
	if old != nil {
		s.retireWG.Add(1)
		go func() {
			defer s.retireWG.Done()
			_ = old.Close()
		}()
	}
	s.Unlock()
	return nil
}

// openVerifiedDB opens one candidate and verifies it with a single
// bounded Ping. Shared by Dial (initial handle) and Recover
// (replacement handle) so the two paths can never diverge in what
// counts as usable. A failure owns no handle: the candidate is
// closed before return.
func openVerifiedDB(url string, ctx context.Context) (*sql.DB, error) {
	db, err := openDB(url)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		// A database URL can be syntactically valid while the database is
		// temporarily unreachable. Let the connection pool retry this case.
		return nil, errorx.NewIOErr(fmt.Sprintf("create connection err:%v", err))
	}
	return db, nil
}

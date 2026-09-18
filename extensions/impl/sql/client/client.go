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
}

const defaultDialTimeout = 2 * time.Second

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
	dialCtx, cancel := context.WithTimeout(ctx, defaultDialTimeout)
	defer cancel()
	return s.dial(dialCtx)
}

func (s *SQLConnection) Reconnect(ctx api.StreamContext) error {
	s.Lock()
	defer s.Unlock()
	dialCtx, cancel := context.WithTimeout(ctx, defaultDialTimeout)
	defer cancel()
	if s.db != nil {
		if err := s.db.PingContext(dialCtx); err == nil {
			return nil
		} else if dialCtx.Err() != nil {
			return dialCtx.Err()
		}
		_ = s.db.Close()
	}
	if err := s.dial(dialCtx); err != nil {
		return fmt.Errorf("reconnect sql err:%v", err)
	}
	return nil
}

func (s *SQLConnection) GetDB() *sql.DB {
	s.RLock()
	defer s.RUnlock()
	return s.db
}

func (s *SQLConnection) Ping(ctx api.StreamContext) error {
	s.Lock()
	defer s.Unlock()
	pingCtx, cancel := context.WithTimeout(ctx, defaultDialTimeout)
	defer cancel()
	if s.db == nil {
		err := s.dial(pingCtx)
		if err != nil {
			return err
		}
	}
	return s.db.PingContext(pingCtx)
}

func (s *SQLConnection) DetachSub(ctx api.StreamContext, props map[string]any) {
	// do nothing
}

func (s *SQLConnection) Close(ctx api.StreamContext) error {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return nil
	}
	ctx.GetLogger().Infof("close db with url:%v", s.url)
	if s.db != nil {
		_ = s.db.Close()
	}
	s.closed = true
	return nil
}

func CreateConnection(ctx api.StreamContext) modules.Connection {
	return &SQLConnection{}
}

func (s *SQLConnection) dial(ctx context.Context) error {
	db, err := openDB(s.url)
	if err != nil {
		return fmt.Errorf("create connection err:%v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		// A database URL can be syntactically valid while the database is
		// temporarily unreachable. Let the connection pool retry this case.
		return errorx.NewIOErr(fmt.Sprintf("create connection err:%v", err))
	}
	s.db = db
	return nil
}

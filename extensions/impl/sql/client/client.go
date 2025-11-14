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
	"database/sql"
	"fmt"
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type SQLConnection struct {
	sync.RWMutex
	url    string
	db     *sql.DB
	id     string
	closed bool
}

func (s *SQLConnection) Provision(ctx api.StreamContext, conId string, props map[string]any) error {
	url, ok := props["url"]
	if !ok {
		url, ok = props["dburl"]
		if !ok {
			return fmt.Errorf("dburl should be defined")
		}
	}
	dburl, ok := url.(string)
	if !ok || len(dburl) < 1 {
		return fmt.Errorf("dburl should be defined as string")
	}
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
	return s.dial(ctx)
}

func (s *SQLConnection) Reconnect() error {
	s.Lock()
	defer s.Unlock()
	if err := s.db.Ping(); err == nil {
		return nil
	}
	oldDB := s.db
	oldDB.Close()
	db, err := openDB(s.url)
	if err != nil {
		return fmt.Errorf("reconnect sql err:%v", err)
	}
	s.db = db
	return s.db.Ping()
}

func (s *SQLConnection) GetDB() *sql.DB {
	s.RLock()
	defer s.RUnlock()
	return s.db
}

func (s *SQLConnection) Ping(ctx api.StreamContext) error {
	s.Lock()
	defer s.Unlock()
	if s.db == nil {
		err := s.dial(ctx)
		if err != nil {
			return err
		}
	}
	return s.db.Ping()
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
	s.db.Close()
	s.closed = true
	return nil
}

func CreateConnection(ctx api.StreamContext) modules.Connection {
	return &SQLConnection{}
}

func (s *SQLConnection) dial(ctx api.StreamContext) error {
	db, err := openDB(s.url)
	if err != nil {
		return fmt.Errorf("create connection err:%v", err)
	}
	s.db = db
	return s.db.Ping()
}

// Copyright 2021 INTECH Process Automation Ltd.
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

package redis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
)

type Instance struct {
	ConnectionString string
	pool             *redis.Pool
	mu               *sync.Mutex
	config           Config
}

func NewRedisFromConf(conf Config) Instance {
	host := conf.Host
	port := conf.Port
	return Instance{
		ConnectionString: connectionString(host, port),
		pool:             nil,
		mu:               &sync.Mutex{},
		config:           conf,
	}
}

func NewRedis(host string, port int) Instance {
	return Instance{
		ConnectionString: connectionString(host, port),
		pool:             nil,
		mu:               &sync.Mutex{},
	}
}

func (r *Instance) Connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ConnectionString == "" {
		return fmt.Errorf("connection string for redis not initalized")
	}
	err, pool := r.connectRedis()
	if err != nil {
		return err
	}
	conn := pool.Get()
	defer conn.Close()
	reply, err := conn.Do("PING")
	if err != nil {
		return err
	}
	response, err := redis.String(reply, err)
	if err != nil {
		return err
	}
	if response != "PONG" {
		return fmt.Errorf("failed to connect to redis")
	}
	r.pool = pool
	return nil
}

func (r *Instance) connectRedis() (error, *redis.Pool) {
	opts := []redis.DialOption{
		redis.DialConnectTimeout(time.Duration(r.config.Timeout) * time.Millisecond),
	}
	if r.config.Password != "" {
		opts = append(opts, redis.DialPassword(r.config.Password))
	}
	dialFunction := func() (redis.Conn, error) {
		conn, err := redis.Dial("tcp", r.ConnectionString, opts...)
		if err == nil {
			_, err = conn.Do("PING")
			if err == nil {
				return conn, nil
			}
		}
		return nil, fmt.Errorf("could not dial redis: %s", err)
	}
	pool := &redis.Pool{
		IdleTimeout: 0,
		MaxIdle:     10,
		Dial:        dialFunction,
	}
	return nil, pool
}

func connectionString(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func (r *Instance) Disconnect() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r == nil {
		return nil
	}
	err := r.pool.Close()
	r.pool = nil
	return err
}

func (r *Instance) Apply(f func(conn redis.Conn) error) error {
	connection := r.pool.Get()
	defer connection.Close()
	return f(connection)
}

// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package lookup

import (
	"fmt"
	"sync/atomic"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	nodeConf "github.com/lf-edge/ekuiper/v2/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

// Table is a lookup table runtime instance. It will run once the table is created.
// It will only stop once the table is dropped.

type info struct {
	ls    api.Source
	count int32
}

var (
	instances = make(map[string]*info)
	lock      = &syncx.Mutex{}
)

// Attach called by lookup nodes. Add a count to the info
func Attach(name string) (api.Source, error) {
	lock.Lock()
	defer lock.Unlock()
	if i, ok := instances[name]; ok {
		atomic.AddInt32(&i.count, 1)
		return i.ls, nil
	}
	return nil, fmt.Errorf("lookup table %s is not found", name)
}

// Detach called by lookup nodes when it is closed
func Detach(name string) error {
	lock.Lock()
	defer lock.Unlock()
	if i, ok := instances[name]; ok {
		atomic.AddInt32(&i.count, -1)
		return nil
	}
	return fmt.Errorf("lookup table %s is not found", name)
}

// CreateInstance called when create a lookup table
func CreateInstance(name string, sourceType string, options *ast.Options) error {
	lock.Lock()
	defer lock.Unlock()
	contextLogger := conf.Log.WithField("table", name)
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	props := nodeConf.GetSourceConf(sourceType, options)
	ctx.GetLogger().Infof("open lookup table with props %v", conf.Printable(props))
	// Create the lookup source according to the source options
	ns, err := io.LookupSource(sourceType)
	if err != nil {
		ctx.GetLogger().Error(err)
		return err
	}
	ctx.GetLogger().Debugf("lookup source %s is created", sourceType)
	err = ns.Provision(ctx, props)
	if err != nil {
		return err
	}
	ctx.GetLogger().Debugf("lookup source %s is configured", sourceType)
	// TODO lookup table connection status support
	err = ns.Connect(ctx, func(status string, message string) {
		// do nothing
	})
	if err != nil {
		return err
	}
	ctx.GetLogger().Debugf("lookup source %s is opened", sourceType)
	instances[name] = &info{ls: ns, count: 0}
	return nil
}

// DropInstance called when drop a lookup table
func DropInstance(name string) error {
	lock.Lock()
	defer lock.Unlock()
	if i, ok := instances[name]; ok {
		if atomic.LoadInt32(&i.count) > 0 {
			return fmt.Errorf("lookup table %s is still in use, stop all using rules before dropping it", name)
		}
		delete(instances, name)
		contextLogger := conf.Log
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		return i.ls.Close(ctx)
	} else {
		return nil
	}
}

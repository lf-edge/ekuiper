// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	nodeConf "github.com/lf-edge/ekuiper/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"sync"
	"sync/atomic"
)

// Table is a lookup table runtime instance. It will run once the table is created.
// It will only stop once the table is dropped.

type info struct {
	ls    api.LookupSource
	count int32
}

var (
	instances = make(map[string]*info)
	lock      = &sync.Mutex{}
)

// Attach called by lookup nodes. Add a count to the info
func Attach(name string) (api.LookupSource, error) {
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
	props := nodeConf.GetSourceConf(ctx, sourceType, options)
	ctx.GetLogger().Infof("open lookup table with props %v", conf.Printable(props))
	// Create the lookup source according to the source options
	ns, err := io.LookupSource(sourceType)
	if err != nil {
		return err
	}
	err = ns.Configure(options.DATASOURCE, props)
	if err != nil {
		return err
	}
	err = ns.Open(ctx)
	if err != nil {
		return err
	}
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
		return nil
	} else {
		return fmt.Errorf("lookup table %s is not found", name)
	}
}

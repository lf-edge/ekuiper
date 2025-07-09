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
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	nodeConf "github.com/lf-edge/ekuiper/v2/internal/topo/node/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

// Table is a lookup table runtime instance. It will run once the table is created.
// It will only stop once the table is dropped.

type info struct {
	ls         api.Source
	count      int32
	sourceType string
	options    *ast.Options
}

var (
	instances = make(map[string]*info)
	lock      = &sync.Mutex{}
)

// Attach called by lookup nodes. Add a count to the info
func Attach(name string) (api.Source, error) {
	lock.Lock()
	defer lock.Unlock()
	if i, ok := instances[name]; ok {
		if i.count < 1 {
			if err := enableInstance(name, i); err != nil {
				return nil, err
			}
		}
		i.count++
		return i.ls, nil
	}
	return nil, fmt.Errorf("lookup table %s is not found", name)
}

func IsEnable(name string) bool {
	lock.Lock()
	defer lock.Unlock()
	instance, ok := instances[name]
	if !ok {
		return false
	}
	return instance.ls != nil
}

// Detach called by lookup nodes when it is closed
func Detach(name string) error {
	lock.Lock()
	defer lock.Unlock()
	if i, ok := instances[name]; ok {
		i.count--
		if i.count < 1 {
			return disableInstance(name, i)
		}
		return nil
	}
	return fmt.Errorf("lookup table %s is not found", name)
}

// CreateInstance called when create a lookup table
func CreateInstance(name string, sourceType string, options *ast.Options) error {
	lock.Lock()
	defer lock.Unlock()
	instances[name] = &info{sourceType: sourceType, count: 0, options: options}
	return nil
}

// DropInstance called when drop a lookup table
func DropInstance(name string) error {
	lock.Lock()
	defer lock.Unlock()
	if i, ok := instances[name]; ok {
		if i.count > 0 {
			return fmt.Errorf("lookup table %s is still in use, stop all using rules before dropping it", name)
		}
		delete(instances, name)
	}
	return nil
}

func disableInstance(name string, instance *info) error {
	contextLogger := conf.Log
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	instance.ls.Close(ctx)
	instance.ls = nil
	return nil
}

func enableInstance(name string, instance *info) error {
	sourceType := instance.sourceType
	options := instance.options
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
	instance.ls = ns
	return nil
}

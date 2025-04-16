// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package file

import (
	"github.com/fsnotify/fsnotify"
	"github.com/lf-edge/ekuiper/contract/v2/api"
)

type WatchWrapper struct {
	f *Source
}

func (f *WatchWrapper) SetEofIngest(eof api.EOFIngest) {
	f.f.SetEofIngest(eof)
}

func (f *WatchWrapper) Provision(ctx api.StreamContext, configs map[string]any) error {
	return f.f.Provision(ctx, configs)
}

func (f *WatchWrapper) Close(ctx api.StreamContext) error {
	return f.f.Close(ctx)
}

func (f *WatchWrapper) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	return f.f.Connect(ctx, sch)
}

func (f *WatchWrapper) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	f.f.Load(ctx, ingest, ingestError)
	ctx.GetLogger().Infof("file watch loaded initially")
	if f.f.isDir {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return err
		}
		err = watcher.Add(f.f.file)
		if err != nil {
			return err
		}
		go func() {
			defer watcher.Close()
			for {
				select {
				case <-ctx.Done():
					return
				case event := <-watcher.Events:
					switch {
					case event.Has(fsnotify.Create), event.Has(fsnotify.Write):
						ctx.GetLogger().Debugf("file watch receive %v", event)
						f.f.parseFile(ctx, event.Name, ingest, ingestError)
					}
				case err = <-watcher.Errors:
					ctx.GetLogger().Errorf("file watch err:%v", err.Error())
				}
			}
		}()
	} else {
		ctx.GetLogger().Infof("file watch exit")
		if f.f != nil && f.f.eof != nil {
			f.f.eof(ctx)
		}
	}
	return nil
}

var (
	_ api.TupleSource = &WatchWrapper{}
	_ api.Bounded     = &WatchWrapper{}
)

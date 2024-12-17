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

package file

import (
	"github.com/fsnotify/fsnotify"
	"github.com/lf-edge/ekuiper/contract/v2/api"
)

func (fs *Source) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	fs.Load(ctx, ingest, ingestError)
	ctx.GetLogger().Infof("file watch loaded initially")
	if fs.isDir {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return err
		}
		err = watcher.Add(fs.file)
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
					//case event.Has(fsnotify.Write):
					case event.Has(fsnotify.Create):
						ctx.GetLogger().Debugf("file watch receive creat event")
						fs.parseFile(ctx, event.Name, ingest, ingestError)
					}
				case err = <-watcher.Errors:
					ctx.GetLogger().Errorf("file watch err:%v", err.Error())
				}
			}
		}()
	} else {
		ctx.GetLogger().Infof("file watch exit")
		fs.eof(ctx)
	}
	return nil
}

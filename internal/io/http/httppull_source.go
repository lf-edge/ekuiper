// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package http

import (
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type PullSource struct {
	ClientConf
}

func (hps *PullSource) Configure(device string, props map[string]interface{}) error {
	conf.Log.Infof("Initialized Httppull source with configurations %#v.", props)
	return hps.InitConf(device, props)
}

func (hps *PullSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	ctx.GetLogger().Infof("Opening HTTP pull source with conf %+v", hps.config)
	hps.initTimerPull(ctx, consumer, errCh)
}

func (hps *PullSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing HTTP pull source")
	return nil
}

func (hps *PullSource) initTimerPull(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	logger := ctx.GetLogger()
	logger.Infof("Starting HTTP pull source with interval %d", hps.config.Interval)
	ticker := time.NewTicker(time.Millisecond * time.Duration(hps.config.Interval))
	defer ticker.Stop()
	omd5 := ""
	for {
		select {
		case <-ticker.C:
			rcvTime := conf.GetNow()
			headers, err := hps.parseHeaders(ctx, hps.tokens)
			if err != nil {
				continue
			}
			// check oAuth token expiration
			if hps.accessConf != nil && hps.accessConf.ExpireInSecond > 0 &&
				int(time.Now().Sub(hps.tokenLastUpdateAt).Abs().Seconds()) >= hps.accessConf.ExpireInSecond {
				ctx.GetLogger().Debugf("Refreshing token")
				if err := hps.refresh(ctx); err != nil {
					ctx.GetLogger().Warnf("Refresh token error: %v", err)
				}
			}
			ctx.GetLogger().Debugf("rest sink sending request url: %s, headers: %v, body %s", hps.config.Url, headers, hps.config.Body)
			if resp, e := httpx.Send(logger, hps.client, hps.config.BodyType, hps.config.Method, hps.config.Url, headers, true, []byte(hps.config.Body)); e != nil {
				logger.Warnf("Found error %s when trying to reach %v ", e, hps)
			} else {
				logger.Debugf("rest sink got response %v", resp)
				results, _, e := hps.parseResponse(ctx, resp, true, &omd5)
				if e != nil {
					logger.Errorf("Parse response error %v", e)
					continue
				}
				if results == nil {
					logger.Debugf("no data to send for incremental")
					continue
				}
				meta := make(map[string]interface{})
				for _, result := range results {
					select {
					case consumer <- api.NewDefaultSourceTupleWithTime(result, meta, rcvTime):
						logger.Debugf("send data to device node")
					case <-ctx.Done():
						return
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

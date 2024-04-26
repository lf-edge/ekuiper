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
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type pullTimeMeta struct {
	LastPullTime int64 `json:"lastPullTime"`
	PullTime     int64 `json:"pullTime"`
}

type PullSource struct {
	ClientConf

	t *pullTimeMeta
}

func (hps *PullSource) Configure(device string, props map[string]interface{}) error {
	conf.Log.Infof("Initialized Httppull source with configurations %#v.", props)
	return hps.InitConf(device, props, WithCheckInterval(true))
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
	ticker := conf.GetTicker(int64(hps.config.Interval))
	defer ticker.Stop()
	omd5 := ""

	// Pulling data at initial start
	logger.Debugf("Pulling data at initial start")
	tuples := hps.doPull(ctx, conf.GetNow(), &omd5)
	io.ReceiveTuples(ctx, consumer, tuples)

	for {
		select {
		case rcvTime := <-ticker.C:
			logger.Debugf("Pulling data at %d", rcvTime.UnixMilli())
			tuples := hps.doPull(ctx, rcvTime, &omd5)
			io.ReceiveTuples(ctx, consumer, tuples)
		case <-ctx.Done():
			return
		}
	}
}

func (hps *PullSource) doPull(ctx api.StreamContext, rcvTime time.Time, omd5 *string) []api.SourceTuple {
	if hps.t == nil {
		hps.t = &pullTimeMeta{
			LastPullTime: rcvTime.UnixMilli() - int64(hps.config.Interval),
			PullTime:     rcvTime.UnixMilli(),
		}
	} else {
		// only update last pull time when there is no error
		hps.t.PullTime = rcvTime.UnixMilli()
	}
	// Parse body which may contain dynamic time range and tokens, so merge them
	var tempProps map[string]any
	if hps.tokens != nil {
		tempProps = hps.tokens
	} else {
		tempProps = make(map[string]any)
	}
	tempProps["LastPullTime"] = hps.t.LastPullTime
	tempProps["PullTime"] = hps.t.PullTime
	// Parse url which may contain dynamic time range
	url, err := ctx.ParseTemplate(hps.config.Url, tempProps)
	if err != nil {
		return []api.SourceTuple{
			&xsql.ErrorSourceTuple{
				Error: fmt.Errorf("parse url %s error %v", hps.config.Url, err),
			},
		}
	}

	// check oAuth token expiration
	if hps.accessConf != nil && hps.accessConf.ExpireInSecond > 0 &&
		int(time.Now().Sub(hps.tokenLastUpdateAt).Abs().Seconds()) >= hps.accessConf.ExpireInSecond {
		ctx.GetLogger().Debugf("Refreshing token for HTTP pull")
		if err := hps.refresh(ctx); err != nil {
			ctx.GetLogger().Warnf("Refresh HTTP pull token error: %v", err)
		}
	}
	headers, err := hps.parseHeaders(ctx, tempProps)
	if err != nil {
		return []api.SourceTuple{
			&xsql.ErrorSourceTuple{
				Error: fmt.Errorf("parse headers error %v", err),
			},
		}
	}
	body, err := ctx.ParseTemplate(hps.config.Body, tempProps)
	if err != nil {
		return []api.SourceTuple{
			&xsql.ErrorSourceTuple{
				Error: fmt.Errorf("parse body %s error %v", hps.config.Body, err),
			},
		}
	}
	ctx.GetLogger().Debugf("httppull source sending request url: %s, headers: %v, body %s", url, headers, hps.config.Body)
	if resp, e := httpx.Send(ctx.GetLogger(), hps.client, url, hps.config.Method,
		httpx.WithHeadersMap(headers),
		httpx.WithBody(body, hps.config.BodyType, true, hps.compressor, hps.config.Compression)); e != nil {
		ctx.GetLogger().Warnf("Found error %s when trying to reach %v ", e, hps)
		return []api.SourceTuple{
			&xsql.ErrorSourceTuple{
				Error: fmt.Errorf("send request error %v", e),
			},
		}
	} else {
		ctx.GetLogger().Debugf("httppull source got response %v", resp)
		results, _, e := hps.parseResponse(ctx, resp, true, omd5, false)
		if e != nil {
			return []api.SourceTuple{
				&xsql.ErrorSourceTuple{
					Error: fmt.Errorf("parse response error %v", e),
				},
			}
		}
		hps.t.LastPullTime = hps.t.PullTime
		if results == nil {
			ctx.GetLogger().Debugf("no data to send for incremental")
			return nil
		}
		tuples := make([]api.SourceTuple, len(results))
		meta := make(map[string]interface{})
		for i, result := range results {
			tuples[i] = api.NewDefaultSourceTupleWithTime(result, meta, rcvTime)
		}
		return tuples
	}
}

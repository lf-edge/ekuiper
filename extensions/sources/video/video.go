// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	ffmpeg "github.com/u2takey/ffmpeg-go"
)

const RTSP_DEFAULT_INTERVAL = 10000
const FRAMENUMBER = 5

type VideoPullSource struct {
	url      string
	interval int
}

func (rps *VideoPullSource) Configure(_ string, props map[string]interface{}) error {

	if u, ok := props["url"]; ok {
		if p, ok := u.(string); ok {
			rps.url = p
		}
	}

	rps.interval = RTSP_DEFAULT_INTERVAL
	if i, ok := props["interval"]; ok {
		i1, err := cast.ToInt(i, cast.CONVERT_SAMEKIND)
		if err != nil {
			return fmt.Errorf("not valid interval value %v", i1)
		} else {
			rps.interval = i1
		}
	}

	return nil
}

func (rps *VideoPullSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	rps.initTimerPull(ctx, consumer, errCh)
}

func (rps *VideoPullSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing video pull source")

	return nil
}

func (rps *VideoPullSource) initTimerPull(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(rps.interval))
	logger := ctx.GetLogger()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rcvTime := conf.GetNow()
			buf := rps.readFrameAsJpeg(ctx)
			results, e := ctx.DecodeIntoList(buf.Bytes())
			meta := make(map[string]interface{})
			if e != nil {
				logger.Errorf("Invalid data format, cannot decode %s with error %s", string(buf.Bytes()), e)
				return
			}
			for _, result := range results {
				select {
				case consumer <- api.NewDefaultSourceTupleWithTime(result, meta, rcvTime):
					logger.Debugf("send data to device node")
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (rps *VideoPullSource) readFrameAsJpeg(ctx api.StreamContext) *bytes.Buffer {
	logger := ctx.GetLogger()
	buf := bytes.NewBuffer(nil)
	err := ffmpeg.Input(rps.url).
		Filter("select", ffmpeg.Args{fmt.Sprintf("gte(n,%d)", FRAMENUMBER)}).
		Output("pipe:", ffmpeg.KwArgs{"vframes": 1, "format": "image2", "vcodec": "mjpeg"}).
		WithOutput(buf, os.Stdout).
		Run()
	if err != nil {
		logger.Errorf("ffmpeg exec error %v", err)
		return buf
	}
	return buf
}

func Video() api.Source {
	return &VideoPullSource{}
}

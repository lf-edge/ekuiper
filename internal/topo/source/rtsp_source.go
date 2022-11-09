// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package source

import (
	"bytes"
	"fmt"
	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/cgo/ffmpeg"
	"github.com/deepch/vdk/format/rtspv2"
	"image/jpeg"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

const RTSP_DEFAULT_INTERVAL = 10000
const RTSP_DEFAULT_TIMEOUT = 3

type RTSPPullSource struct {
	url        string
	interval   int
	timeout    int
	rtspClient *rtspv2.RTSPClient
	vedioDe    *ffmpeg.VideoDecoder
	videoIDX   int
}

func (rps *RTSPPullSource) Configure(_ string, props map[string]interface{}) error {

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

	rps.timeout = RTSP_DEFAULT_TIMEOUT
	if i, ok := props["timeout"]; ok {
		if i1, ok1 := i.(int); ok1 {
			rps.timeout = i1
		} else {
			return fmt.Errorf("not valid timeout value %v", i1)
		}
	}

	RTSPClient, err := rtspv2.Dial(rtspv2.RTSPClientOptions{URL: rps.url, DisableAudio: false, DialTimeout: time.Duration(rps.timeout) * time.Second, ReadWriteTimeout: time.Duration(rps.timeout) * time.Second, Debug: false})
	if err != nil {
		return err
	}
	var videoIDX int
	AudioOnly := true
	for i, codec := range RTSPClient.CodecData {
		if codec.Type().IsVideo() {
			AudioOnly = false
		}
		if codec.Type().IsVideo() {
			videoIDX = i
		}
	}
	if AudioOnly {
		return fmt.Errorf("audo only rtsp stream, no vedio %#v", rps)
	}

	FrameDecoderSingle, err := ffmpeg.NewVideoDecoder(RTSPClient.CodecData[videoIDX].(av.VideoCodecData))
	if err != nil {
		return fmt.Errorf("ffmpeg get NewVideoDecoder error %v", err)
	}
	rps.videoIDX = videoIDX
	rps.vedioDe = FrameDecoderSingle
	rps.rtspClient = RTSPClient
	conf.Log.Debugf("Initialized with configurations %#v.", rps)
	return nil
}

func (rps *RTSPPullSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	rps.initTimerPull(ctx, consumer, errCh)
}

func (rps *RTSPPullSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing HTTP pull source")
	if rps.rtspClient != nil {
		rps.rtspClient.Close()
	}
	return nil
}

func (rps *RTSPPullSource) initTimerPull(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(rps.interval))
	logger := ctx.GetLogger()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:

		case packetAV := <-rps.rtspClient.OutgoingPacketQueue:
			if packetAV.IsKeyFrame && packetAV.Idx == int8(rps.videoIDX) {
				if pic, err := rps.vedioDe.DecodeSingle(packetAV.Data); err == nil && pic != nil {
					buf := new(bytes.Buffer)
					if err = jpeg.Encode(buf, &pic.Image, nil); err == nil {
						result, e := ctx.Decode(buf.Bytes())
						meta := make(map[string]interface{})
						if e != nil {
							logger.Errorf("Invalid data format, cannot decode %s with error %s", string(buf.Bytes()), e)
							break
						}
						logger.Infof("send data to device node %s\n", string(buf.Bytes()))

						select {
						case consumer <- api.NewDefaultSourceTuple(result, meta):
							logger.Debugf("send data to device node")
						case <-ctx.Done():
							return
						}
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

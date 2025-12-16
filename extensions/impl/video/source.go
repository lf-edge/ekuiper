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

package video

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	ffmpeg "github.com/u2takey/ffmpeg-go"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)


type Source struct {
	Url string `json:"url"`
	// Run ffmpeg -formats to get all supported format, default to 'image2'
	Format string `json:"vformat"`
	// Check https://www.ffmpeg.org/general.html#Video-Codecs, default to 'mjpeg'
	Codec     string         `json:"codec"`
	DebugResp bool           `json:"debugResp"`
	InputArgs map[string]any `json:"inputArgs"`
	meta      map[string]any
}

func (s *Source) Provision(ctx api.StreamContext, props map[string]any) error {
	c := exec.Command("ffmpeg", "-version")
	output, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("check ffmpeg failed, err:%v", err)
	}
	ctx.GetLogger().Infof("ffmpeg dependency check result: %s", string(output))
	if strings.Contains(string(output), "ffmpeg version") {
		ctx.GetLogger().Infof("ffmpeg dependency check success")
	} else {
		return errors.New("ffmpeg dependency check failed")
	}

	s.Format = "image2"
	s.Codec = "mjpeg"
	err = cast.MapToStruct(props, s)
	if err != nil {
		return err
	}
	if s.Url == "" {
		return errors.New("url is empty")
	}
	_, err = url.ParseRequestURI(s.Url)
	if err != nil {
		return fmt.Errorf("url is invalid, err:%v", err)
	}
	s.meta = map[string]any{"url": s.Url}
	return nil
}

func (s *Source) Close(_ api.StreamContext) error {
	// do nothing
	return nil
}

func (s *Source) Connect(_ api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnected, "")
	return nil
}

func (s *Source) Pull(ctx api.StreamContext, trigger time.Time, ingest api.BytesIngest, ingestError api.ErrorIngest) {
	buf, err := s.readFrameAsJpeg(ctx)
	if err != nil {
		ingestError(ctx, err)
	} else {
		ingest(ctx, buf.Bytes(), s.meta, trigger)
	}
}

func (s *Source) readFrameAsJpeg(ctx api.StreamContext) (*bytes.Buffer, error) {
	ctx.GetLogger().Debugf("read frame at %v", time.Now())
	buf := bytes.NewBuffer(nil)
	err := s.readTo(ctx, buf)
	if err != nil {
		return nil, fmt.Errorf("read frame failed, err:%v", err)
	}
	return buf, nil
}

func (s *Source) readTo(ctx api.StreamContext, out io.Writer) error {
	ctx.GetLogger().Debugf("read frame at %v", time.Now())
	stream := ffmpeg.Input(s.Url, s.InputArgs).
		Output("pipe:", ffmpeg.KwArgs{"vframes": 1, "format": s.Format, "vcodec": s.Codec}).
		WithOutput(out)
	if s.DebugResp {
		var errBuf bytes.Buffer
		stream = stream.WithErrorOutput(&errBuf)
		err := stream.Run()
		ctx.GetLogger().Infof("ffmpeg output: %s", errBuf.String())
		return err
	}
	return stream.Run()
}

func GetSource() api.Source {
	return &Source{}
}

var _ api.PullBytesSource = &Source{}

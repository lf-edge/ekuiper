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
	"bufio"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	ffmpeg "github.com/u2takey/ffmpeg-go"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

type Source struct {
	Url string `json:"url"`
	// Run ffmpeg -formats to get all supported format, default to 'image2'
	Format string `json:"vformat"`
	// Check https://www.ffmpeg.org/general.html#Video-Codecs, default to 'mjpeg'
	Codec     string            `json:"codec"`
	DebugResp bool              `json:"debugResp"`
	InputArgs map[string]any    `json:"inputArgs"`
	Interval  cast.DurationConf `json:"interval"`
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

	s.Format = "image2pipe"
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

func (s *Source) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	ctx.GetLogger().Infof("start video source subscribe with interval %s", s.Interval)
	var fps string
	if s.Interval > 0 {
		fps = fmt.Sprintf("1/%f", time.Duration(s.Interval).Seconds())
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			start := time.Now()
			err := s.runCurrent(ctx, fps, ingest)
			if err != nil {
				// check if recoverable
				// If process exit too fast (less than 2s) with specific error, return error
				if time.Since(start) < 2*time.Second && isFatalError(err) {
					ingestError(ctx, err)
					return err
				}
				ctx.GetLogger().Errorf("ffmpeg run failed: %v, restarting...", err)
				ingestError(ctx, err)
			}
			// backoff
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1 * time.Second):
			}
		}
	}
}

func isFatalError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Invalid argument") || strings.Contains(msg, "Option not found")
}

func (s *Source) runCurrent(ctx api.StreamContext, fps string, ingest api.BytesIngest) error {
	input := ffmpeg.Input(s.Url, s.InputArgs)
	if fps != "" {
		input = input.Filter("fps", ffmpeg.Args{fps})
	}
	cmd := input.Output("pipe:", ffmpeg.KwArgs{
		"f":      s.Format,
		"vcodec": s.Codec,
		"q:v":    "2",
	}).Compile()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			if s.DebugResp {
				ctx.GetLogger().Infof("ffmpeg stderr: %s", scanner.Text())
			}
		}
	}()

	scanner := bufio.NewScanner(stdout)
	scanner.Split(splitJPEGs)
	// Larger buffer for high res images
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024*10)

	for scanner.Scan() {
		data := scanner.Bytes()
		// Copy data because scanner bytes are reused
		out := make([]byte, len(data))
		copy(out, data)
		ingest(ctx, out, s.meta, time.Now())
	}

	if err := scanner.Err(); err != nil {
		ctx.GetLogger().Errorf("scanner error: %v", err)
	}

	// wait for cmd to exit
	select {
	case <-ctx.Done():
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return nil
	case err := <-done:
		return err
	}
}

func (s *Source) Info() model.NodeInfo {
	return model.NodeInfo{
		HasInterval:     true,
		NeedDecode:      true,
		NeedBatchDecode: true,
	}
}

func (s *Source) TransformType() api.Source {
	return s
}

func GetSource() api.Source {
	return &Source{}
}

var (
	_ api.BytesSource = &Source{}
	_ model.InfoNode  = &Source{}
)

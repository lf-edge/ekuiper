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

package metrics

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/utahta/go-cronowriter"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

const OpenMetricsEOF = "# EOF\n"

func InitMetricsDumpJob(ctx context.Context) {
	metricsManager.Init(ctx)
}

func GetMetricsZipFile(startTime time.Time, endTime time.Time) (string, error) {
	if !metricsManager.enabeld {
		return "", fmt.Errorf("metrics dump not enabled")
	}
	return metricsManager.dumpMetricsFile(startTime, endTime)
}

var metricsManager = &MetricsDumpManager{}

type MetricsDumpManager struct {
	enabeld          bool
	writer           *cronowriter.CronoWriter
	metricsPath      string
	retainedDuration time.Duration
	regex            *regexp.Regexp
}

func (m *MetricsDumpManager) Init(ctx context.Context) error {
	if !conf.Config.Basic.MetricsDumpConfig.Enable {
		return nil
	}
	m.enabeld = true
	metricsPath, err := conf.GetMetricsLoc()
	if err != nil {
		return err
	}
	m.metricsPath = metricsPath
	w := cronowriter.MustNew(fmt.Sprintf("%s/metrics.", m.metricsPath) + `%Y%m%d-%H` + `.log`)
	m.writer = w
	m.retainedDuration = conf.Config.Basic.MetricsDumpConfig.RetainedDuration
	m.regex = regexp.MustCompile(`^metrics\.(\d{4})(\d{2})(\d{2})-(\d{2})\.log$`)
	go m.gcOldMetricsJob(ctx)
	go m.dumpMetricsJob(ctx)
	return nil
}

func (m *MetricsDumpManager) gcOldMetricsJob(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.gcOldMetrics()
		}
	}
}

func (m *MetricsDumpManager) gcOldMetrics() error {
	gcTime := time.Now().Add(-m.retainedDuration)
	files, err := os.ReadDir(m.metricsPath)
	if err != nil {
		return fmt.Errorf("Error reading directory: %v\n", err)
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		fileName := f.Name()
		fileTime, err := m.extractFileTime(fileName)
		if err != nil {
			continue
		}
		if fileTime.Before(gcTime) {
			filePath := filepath.Join(m.metricsPath, fileName)
			os.Remove(filePath)
		}
	}
	return nil
}

func (m *MetricsDumpManager) dumpMetricsJob(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.dumpMetrics()
		}
	}
}

func (m *MetricsDumpManager) dumpMetrics() error {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return err
	}
	if len(mfs) < 1 {
		return nil
	}
	now := time.Now().Unix()
	for _, mf := range mfs {
		for index, metric := range mf.Metric {
			metric.TimestampMs = &now
			mf.Metric[index] = metric
		}
		expfmt.MetricFamilyToText(m.writer, mf)
	}
	m.writer.Write([]byte(OpenMetricsEOF))
	conf.Log.Info("dump metrics success")
	return nil
}

func (m *MetricsDumpManager) dumpMetricsFile(startTime time.Time, endTime time.Time) (string, error) {
	files, err := os.ReadDir(m.metricsPath)
	if err != nil {
		return "", fmt.Errorf("Error reading directory: %v\n", err)
	}
	fileNames := make([]string, 0)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		fileTime, err := m.extractFileTime(f.Name())
		if err != nil {
			continue
		}
		if isFileIncludeMetricsTime(fileTime, startTime) || isFileIncludeMetricsTime(fileTime, endTime) {
			fileNames = append(fileNames, f.Name())
		}
	}
	if len(fileNames) < 1 {
		return "", fmt.Errorf("not metrics are selected")
	}
	return m.dumpMetricsFileIntoZip(fileNames)
}

func (m *MetricsDumpManager) dumpMetricsFileIntoZip(filenames []string) (string, error) {
	zipFilePath := filepath.Join(os.TempDir(), "metrics.zip")
	zipFile, err := os.Create(zipFilePath)
	if err != nil {
		panic(err)
	}
	defer zipFile.Close()
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()
	for _, filename := range filenames {
		fileToZip, err := os.Open(filepath.Join(m.metricsPath, filename))
		if err != nil {
			return "", err
		}
		defer fileToZip.Close()
		info, err := fileToZip.Stat()
		if err != nil {
			return "", err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return "", err
		}
		header.Name = filepath.Base(filename)
		header.Method = zip.Deflate
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return "", err
		}
		_, err = io.Copy(writer, fileToZip)
		if err != nil {
			return "", err
		}
	}
	zipWriter.Flush()
	return zipFilePath, nil
}

func (m *MetricsDumpManager) extractFileTime(fileName string) (time.Time, error) {
	if !strings.HasPrefix(fileName, "metrics") || !m.regex.MatchString(fileName) {
		return time.Time{}, fmt.Errorf("invalid metrics file name: %s", fileName)
	}
	matches := m.regex.FindStringSubmatch(fileName)
	if len(matches) < 4 {
		return time.Time{}, fmt.Errorf("invalid metrics file name: %s", fileName)
	}
	year := matches[1]
	month := matches[2]
	day := matches[3]
	hour := matches[4]
	fileTime, err := time.ParseInLocation("20060102-15", fmt.Sprintf("%s%s%s-%s", year, month, day, hour), cast.GetConfiguredTimeZone())
	if err != nil {
		return time.Time{}, err
	}
	return fileTime, nil
}

func isFileIncludeMetricsTime(fileTime, metricsTime time.Time) bool {
	return fileTime.Before(metricsTime) && fileTime.Add(time.Hour).After(metricsTime)
}

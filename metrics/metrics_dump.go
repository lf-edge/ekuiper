// Copyright 2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/utahta/go-cronowriter"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

const OpenMetricsEOF = "# EOF\n"

func InitMetricsDumpJob(ctx context.Context) {
	metricsManager.Init(ctx)
}

func GetMetricsZipFile(startTime time.Time, endTime time.Time) (string, error) {
	if !metricsManager.IsEnabled() {
		return "", fmt.Errorf("metrics dump not enabled")
	}
	return metricsManager.dumpMetricsFile(startTime, endTime)
}

func IsMetricsDumpEnabled() bool {
	return metricsManager.IsEnabled()
}

func StartMetricsManager() error {
	return metricsManager.Start()
}

func StopMetricsManager() {
	metricsManager.Stop()
}

var metricsManager = &MetricsDumpManager{}

type MetricsDumpManager struct {
	syncx.Mutex
	enabeld          bool
	writer           *cronowriter.CronoWriter
	metricsPath      string
	retainedDuration time.Duration
	regex            *regexp.Regexp
	cancel           context.CancelFunc
	wg               *sync.WaitGroup
	dryRun           bool
}

func (m *MetricsDumpManager) Init(ctx context.Context) error {
	if !conf.Config.Basic.MetricsDumpConfig.Enable {
		conf.Log.Infof("metrics dump disabled")
		return nil
	}
	return m.init(ctx)
}

func (m *MetricsDumpManager) IsEnabled() bool {
	m.Lock()
	defer m.Unlock()
	return m.enabeld
}

func (m *MetricsDumpManager) Stop() {
	m.Lock()
	defer m.Unlock()
	if !m.enabeld {
		return
	}
	m.cancel()
	m.wg.Wait()
	m.enabeld = false
}

func (m *MetricsDumpManager) Start() error {
	m.Lock()
	defer m.Unlock()
	if m.enabeld {
		return nil
	}
	return m.init(context.Background())
}

func (m *MetricsDumpManager) init(parCtx context.Context) error {
	if err := conf.InitMetricsFolder(); err != nil {
		return fmt.Errorf("init metrics folder err:%v", err)
	}
	ctx, cancel := context.WithCancel(parCtx)
	m.cancel = cancel
	m.wg = &sync.WaitGroup{}
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
	m.wg.Add(2)
	go m.gcOldMetricsJob(ctx)
	go m.dumpMetricsJob(ctx)
	conf.Log.Infof("metrics dump enabled, folder:%v, retension:%v", m.metricsPath, m.retainedDuration.String())
	return nil
}

func (m *MetricsDumpManager) gcOldMetricsJob(ctx context.Context) {
	defer func() {
		m.wg.Done()
	}()
	ticker := time.NewTicker(15 * time.Minute)
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
	if m.dryRun {
		return nil
	}
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
		needGC, err := m.needGCFile(fileName, gcTime)
		if err != nil {
			conf.Log.Errorf("check metrics %v failed, err:%v", fileName, err)
			continue
		}
		if needGC {
			filePath := filepath.Join(m.metricsPath, fileName)
			os.Remove(filePath)
			conf.Log.Infof("gc metrics dump file:%v", fileName)
		} else {
			conf.Log.Infof("skip gc metrics dump file:%v", fileName)
		}
	}
	return nil
}

func (m *MetricsDumpManager) needGCFile(filename string, gcTime time.Time) (bool, error) {
	fileTime, err := m.extractFileTime(filename)
	if err != nil {
		return false, err
	}
	if fileTime.Before(gcTime) {
		filePath := filepath.Join(m.metricsPath, filename)
		os.Remove(filePath)
		return true, nil
	}
	return false, nil
}

func (m *MetricsDumpManager) dumpMetricsJob(ctx context.Context) {
	defer func() {
		m.wg.Done()
	}()
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
	if m.dryRun {
		return nil
	}
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
			metric.Label = append(metric.Label, &io_prometheus_client.LabelPair{Name: stringToPtr("instance"), Value: stringToPtr("local")})
			mf.Metric[index] = metric
		}
		expfmt.MetricFamilyToText(m.writer, mf)
	}
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
	openMetricsFile, err := m.writeOpenMetricsIntoFile(filenames)
	if err != nil {
		return "", err
	}
	defer os.Remove(openMetricsFile)
	zipFilePath := filepath.Join(os.TempDir(), "metrics.zip")
	zipFile, err := os.Create(zipFilePath)
	if err != nil {
		return "", err
	}
	defer zipFile.Close()
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()
	of, err := os.Open(openMetricsFile)
	if err != nil {
		return "", err
	}
	defer of.Close()
	info, err := of.Stat()
	if err != nil {
		return "", err
	}
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return "", err
	}
	header.Name = filepath.Base(of.Name())
	header.Method = zip.Deflate
	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(writer, of)
	if err != nil {
		return "", err
	}
	zipWriter.Flush()
	return zipFilePath, nil
}

func (m *MetricsDumpManager) writeOpenMetricsIntoFile(filenames []string) (string, error) {
	f, err := os.CreateTemp(os.TempDir(), "metrics.log.*")
	if err != nil {
		return "", err
	}
	defer f.Close()
	for _, rf := range filenames {
		reader, err := os.Open(filepath.Join(m.metricsPath, rf))
		if err != nil {
			return "", err
		}
		_, err = io.Copy(f, reader)
		if err != nil {
			return "", err
		}
		reader.Close()
	}
	_, err = f.WriteString(OpenMetricsEOF)
	return f.Name(), err
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

func stringToPtr(a string) *string {
	return &a
}

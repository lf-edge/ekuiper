// Copyright 2026 Timecho Limited
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

package iotdb

import (
	"fmt"
	"net"
	"strings"
)

const (
	modelTree  = "tree"
	modelTable = "table"

	categoryTag       = "TAG"
	categoryField     = "FIELD"
	categoryAttribute = "ATTRIBUTE"
)

// supportedDataTypes lists all valid IoTDB data type strings.
var supportedDataTypes = map[string]struct{}{
	"INT32":     {},
	"INT64":     {},
	"FLOAT":     {},
	"DOUBLE":    {},
	"BOOLEAN":   {},
	"TEXT":      {},
	"STRING":    {},
	"TIMESTAMP": {},
}

// supportedCategories lists all valid IoTDB column category strings.
var supportedCategories = map[string]struct{}{
	categoryTag:       {},
	categoryField:     {},
	categoryAttribute: {},
}

// iotdbConfig is the configuration for the IoTDB sink.
type iotdbConfig struct {
	// connection parameters
	Addr     string `json:"addr"`
	Username string `json:"username"`
	Password string `json:"password"`

	// model selection: "tree" or "table"
	Model string `json:"model"`

	// tree model parameters
	Device    string `json:"device"`
	IsAligned bool   `json:"isAligned"`

	// table model parameters
	Database         string   `json:"database"`
	Table            string   `json:"table"`
	ColumnCategories []string `json:"columnCategories"`

	// common data mapping
	Measurements []string `json:"measurements"`
	DataTypes    []string `json:"dataTypes"`

	// common write parameters
	TsFieldName string `json:"tsFieldName"`
	BatchSize   int    `json:"batchSize"`
	Timeout     int64  `json:"timeout"`
	PoolSize    int    `json:"poolSize"`

	// cluster mode (optional)
	NodeUrls []string `json:"nodeUrls"`
}

// applyDefaults fills in default values for unset fields.
func (c *iotdbConfig) applyDefaults() {
	if c.Addr == "" {
		c.Addr = "127.0.0.1:6667"
	}
	if c.Username == "" {
		c.Username = "root"
	}
	if c.Password == "" {
		c.Password = "root"
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10
	}
	if c.Timeout <= 0 {
		c.Timeout = 5000
	}
	if c.PoolSize <= 0 {
		c.PoolSize = 3
	}
}

// validate checks the configuration for required fields and consistency.
func (c *iotdbConfig) validate() error {
	// normalize model
	c.Model = strings.ToLower(strings.TrimSpace(c.Model))
	if c.Model != modelTree && c.Model != modelTable {
		return fmt.Errorf("model must be either %q or %q", modelTree, modelTable)
	}

	if len(c.Measurements) == 0 {
		return fmt.Errorf("measurements is required")
	}
	if len(c.DataTypes) == 0 {
		return fmt.Errorf("dataTypes is required")
	}
	if len(c.Measurements) != len(c.DataTypes) {
		return fmt.Errorf("measurements (%d) and dataTypes (%d) must have the same length",
			len(c.Measurements), len(c.DataTypes))
	}
	for i, dt := range c.DataTypes {
		up := strings.ToUpper(strings.TrimSpace(dt))
		if _, ok := supportedDataTypes[up]; !ok {
			return fmt.Errorf("dataTypes[%d]=%q is not a supported IoTDB data type", i, dt)
		}
		c.DataTypes[i] = up
	}

	switch c.Model {
	case modelTree:
		if c.Device == "" {
			return fmt.Errorf("device is required when model is %q", modelTree)
		}
	case modelTable:
		if c.Database == "" {
			return fmt.Errorf("database is required when model is %q", modelTable)
		}
		// 表模型的 database 不需要 root. 前缀，自动去除
		c.Database = strings.TrimPrefix(strings.TrimSpace(c.Database), "root.")
		if c.Database == "" {
			return fmt.Errorf("database name cannot be empty after stripping 'root.' prefix")
		}
		if c.Table == "" {
			return fmt.Errorf("table is required when model is %q", modelTable)
		}
		if len(c.ColumnCategories) != len(c.Measurements) {
			return fmt.Errorf("columnCategories (%d) must have the same length as measurements (%d)",
				len(c.ColumnCategories), len(c.Measurements))
		}
		for i, cat := range c.ColumnCategories {
			up := strings.ToUpper(strings.TrimSpace(cat))
			if _, ok := supportedCategories[up]; !ok {
				return fmt.Errorf("columnCategories[%d]=%q is not a supported column category (TAG/FIELD/ATTRIBUTE)", i, cat)
			}
			c.ColumnCategories[i] = up
		}
	}
	return nil
}

// splitAddr parses an "host:port" string into its parts. It uses
// net.SplitHostPort so IPv6 literals such as "[::1]:6667" are handled correctly.
func splitAddr(addr string) (host string, port string, err error) {
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		return "", "", fmt.Errorf("invalid addr %q, expected host:port: %w", addr, err)
	}
	if host == "" || port == "" {
		return "", "", fmt.Errorf("invalid addr %q, expected host:port", addr)
	}
	return host, port, nil
}

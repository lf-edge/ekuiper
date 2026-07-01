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

	"github.com/apache/iotdb-client-go/v2/client"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

// toTSDataType maps an IoTDB data type string (already upper-cased and validated)
// to the corresponding client.TSDataType constant.
func toTSDataType(s string) (client.TSDataType, error) {
	switch s {
	case "INT32":
		return client.INT32, nil
	case "INT64":
		return client.INT64, nil
	case "FLOAT":
		return client.FLOAT, nil
	case "DOUBLE":
		return client.DOUBLE, nil
	case "BOOLEAN":
		return client.BOOLEAN, nil
	case "TEXT":
		return client.TEXT, nil
	case "STRING":
		return client.STRING, nil
	case "TIMESTAMP":
		return client.TIMESTAMP, nil
	default:
		return client.UNKNOWN, fmt.Errorf("unsupported IoTDB data type %q", s)
	}
}

// toColumnCategory maps a category string to client.ColumnCategory.
func toColumnCategory(s string) (client.ColumnCategory, error) {
	switch s {
	case categoryTag:
		return client.TAG, nil
	case categoryField:
		return client.FIELD, nil
	case categoryAttribute:
		return client.ATTRIBUTE, nil
	default:
		return 0, fmt.Errorf("unsupported column category %q", s)
	}
}

// buildMeasurementSchemas builds a list of *client.MeasurementSchema for given names and types.
func buildMeasurementSchemas(measurements []string, dataTypes []string) ([]*client.MeasurementSchema, error) {
	schemas := make([]*client.MeasurementSchema, 0, len(measurements))
	for i, m := range measurements {
		dt, err := toTSDataType(dataTypes[i])
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, &client.MeasurementSchema{
			Measurement: m,
			DataType:    dt,
		})
	}
	return schemas, nil
}

// convertValue casts a Go value to the target IoTDB data type's expected Go type.
// Returns nil when the input is nil (caller decides how to handle missing values).
func convertValue(v any, dt string) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch dt {
	case "INT32":
		i, err := cast.ToInt(v, cast.CONVERT_ALL)
		if err != nil {
			return nil, err
		}
		return int32(i), nil
	case "INT64", "TIMESTAMP":
		i, err := cast.ToInt64(v, cast.CONVERT_ALL)
		if err != nil {
			return nil, err
		}
		return i, nil
	case "FLOAT":
		f, err := cast.ToFloat64(v, cast.CONVERT_ALL)
		if err != nil {
			return nil, err
		}
		return float32(f), nil
	case "DOUBLE":
		f, err := cast.ToFloat64(v, cast.CONVERT_ALL)
		if err != nil {
			return nil, err
		}
		return f, nil
	case "BOOLEAN":
		b, err := cast.ToBool(v, cast.CONVERT_ALL)
		if err != nil {
			return nil, err
		}
		return b, nil
	case "TEXT", "STRING":
		s, err := cast.ToString(v, cast.CONVERT_ALL)
		if err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, fmt.Errorf("unsupported IoTDB data type %q", dt)
	}
}

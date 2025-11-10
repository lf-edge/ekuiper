package replace

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplaceRuleJson(t *testing.T) {
	data := `{
  "triggered": true,
  "id": "sql",
  "sql": "SELECT\n  *\nFROM\n  simulator",
  "actions": [
    {
      "sql": {
        "batchSize": 0,
        "bufferLength": 1024,
        "bufferPageSize": 256,
        "cleanCacheAtStop": false,
        "concurrency": 1,
        "enableCache": false,
        "fields": [
          "msgid",
          "topic",
          "qos",
          "payload",
          "arrived"
        ],
        "format": "json",
        "lingerInterval": 0,
        "maxDiskCache": 1024000,
        "memoryCacheThreshold": 1024,
        "omitIfEmpty": false,
        "resendAlterQueue": false,
        "resendInterval": 0,
        "resendPriority": 0,
        "runAsync": false,
        "sendSingle": true,
        "table": "t_mqtt_msg",
        "url": "mysql://emqx:changeme@mysql.intgmysql.svc.cluster.local:3306/emqx_data"
      }
    }
  ],
  "options": {
    "lateTolerance": "1s",
    "concurrency": 1,
    "bufferLength": 1024,
    "sendError": true,
    "checkpointInterval": "5m0s",
    "restartStrategy": {
      "delay": "1s",
      "multiplier": 2,
      "maxDelay": "30s",
      "jitterFactor": 0.1
    }
  }
}`
	got := ReplaceRuleJson(data, false)
	require.False(t, strings.Contains(got, `"url"`))
	require.True(t, strings.Contains(got, `"dburl"`))

	data = `{
    "triggered": true,
    "id": "sql",
    "sql": "SELECT\n  *\nFROM\n  simulator",
    "actions": [
        {
            "kafka": {
                "saslPassword": "123"
            }
        }
    ],
    "options": {
        "lateTolerance": "1s",
        "concurrency": 1,
        "bufferLength": 1024,
        "sendError": true,
        "checkpointInterval": "5m0s",
        "restartStrategy": {
            "delay": "1s",
            "multiplier": 2,
            "maxDelay": "30s",
            "jitterFactor": 0.1
        }
    }
}`
	got = ReplaceRuleJson(data, false)
	require.False(t, strings.Contains(got, `"saslPassword"`))
	require.True(t, strings.Contains(got, `"password"`))

	data = `{
    "triggered": true,
    "id": "sql",
    "sql": "SELECT\n  *\nFROM\n  simulator",
    "actions": [
        {
            "kafka": {
            }
        }
    ],
    "options": {
        "lateTolerance": "1s",
        "concurrency": 1,
        "bufferLength": 1024,
        "sendError": true,
        "checkpointInterval": "5m0s",
        "restartStrategy": {
            "delay": "1s",
            "multiplier": 2,
            "maxDelay": "30s",
            "jitterFactor": 0.1
        }
    }
}`
	got = ReplaceRuleJson(data, false)
	require.Equal(t, data, got)
}

func TestReplaceDuration(t *testing.T) {
	props := map[string]interface{}{
		"timeout": 1000,
	}
	changed, newProps := ReplaceDuration(props)
	require.True(t, changed)
	require.Equal(t, map[string]interface{}{
		"timeout": "1s",
	}, newProps)
	props = map[string]interface{}{
		"timeout": int64(1000),
	}
	changed, newProps = ReplaceDuration(props)
	require.True(t, changed)
	require.Equal(t, map[string]interface{}{
		"timeout": "1s",
	}, newProps)
	props = map[string]interface{}{
		"timeout": float64(1000),
	}
	changed, newProps = ReplaceDuration(props)
	require.True(t, changed)
	require.Equal(t, map[string]interface{}{
		"timeout": "1s",
	}, newProps)
}

func TestRelacePropsPlug(t *testing.T) {
	props := map[string]interface{}{
		"timeout": 1000,
		"url":     "123",
	}
	changed, newProps := ReplacePropsWithPlug("", props)
	require.True(t, changed)
	require.Equal(t, map[string]interface{}{
		"timeout": "1s",
		"url":     "123",
	}, newProps)
	props = map[string]interface{}{
		"timeout": 1000,
		"url":     "123",
	}
	changed, newProps = ReplacePropsWithPlug("sql", props)
	require.True(t, changed)
	require.Equal(t, map[string]interface{}{
		"timeout": "1s",
		"dburl":   "123",
	}, newProps)
}

func TestReplaceCacheTtl(t *testing.T) {
	props := map[string]interface{}{
		"lookup": map[string]interface{}{
			"a":        1,
			"cacheTtl": 100,
		},
		"a": "b",
	}
	changed, newProps := ReplacePropsWithPlug("sql", props)
	require.True(t, changed)
	require.Equal(t, map[string]interface{}{
		"lookup": map[string]interface{}{
			"a":        1,
			"cacheTtl": "100ms",
		},
		"a": "b",
	}, newProps)
}

func TestHidePassword(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected map[string]any
	}{
		{
			name: "top-level password fields",
			input: map[string]any{
				"username": "admin",
				"password": "secret123",
				"token":    "abc123",
			},
			expected: map[string]any{
				"username": "admin",
				"password": "*",
				"token":    "*",
			},
		},
		{
			name: "nested map with password fields",
			input: map[string]any{
				"user": map[string]any{
					"name":     "Alice",
					"password": "secret123",
				},
				"config": map[string]any{
					"access_token": "token123",
				},
			},
			expected: map[string]any{
				"user": map[string]any{
					"name":     "Alice",
					"password": "*",
				},
				"config": map[string]any{
					"access_token": "*",
				},
			},
		},
		{
			name: "deeply nested maps",
			input: map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"refresh_token": "refresh123",
						"data":          "keep",
					},
				},
			},
			expected: map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"refresh_token": "*",
						"data":          "keep",
					},
				},
			},
		},
		{
			name: "non-password fields preserved",
			input: map[string]any{
				"id":       123,
				"active":   true,
				"password": "secret",
				"details": map[string]any{
					"count": 42,
					"pass":  "nested",
				},
			},
			expected: map[string]any{
				"id":       123,
				"active":   true,
				"password": "*",
				"details": map[string]any{
					"count": 42,
					"pass":  "*",
				},
			},
		},
		{
			name:     "empty map",
			input:    map[string]any{},
			expected: map[string]any{},
		},
		{
			name: "no password fields",
			input: map[string]any{
				"foo": "bar",
				"num": 123,
			},
			expected: map[string]any{
				"foo": "bar",
				"num": 123,
			},
		},
		{
			name: "all password variants",
			input: map[string]any{
				"password":      "pwd",
				"pass":          "p",
				"token":         "t",
				"access_token":  "at",
				"refresh_token": "rt",
			},
			expected: map[string]any{
				"password":      "*",
				"pass":          "*",
				"token":         "*",
				"access_token":  "*",
				"refresh_token": "*",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HidePassword(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

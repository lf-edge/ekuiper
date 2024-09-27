package replace

import (
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
	got := ReplaceRuleJson(data)
	require.Equal(t, `{"actions":[{"sql":{"batchSize":0,"bufferLength":1024,"bufferPageSize":256,"cleanCacheAtStop":false,"concurrency":1,"dburl":"mysql://emqx:changeme@mysql.intgmysql.svc.cluster.local:3306/emqx_data","enableCache":false,"fields":["msgid","topic","qos","payload","arrived"],"format":"json","lingerInterval":0,"maxDiskCache":1024000,"memoryCacheThreshold":1024,"omitIfEmpty":false,"resendAlterQueue":false,"resendInterval":0,"resendPriority":0,"runAsync":false,"sendSingle":true,"table":"t_mqtt_msg"}}],"id":"sql","options":{"bufferLength":1024,"checkpointInterval":"5m0s","concurrency":1,"lateTolerance":"1s","restartStrategy":{"delay":"1s","jitterFactor":0.1,"maxDelay":"30s","multiplier":2},"sendError":true},"sql":"SELECT\n  *\nFROM\n  simulator","triggered":true}`, got)
}

# MQTT action

The action is used for publish output message into a MQTT server. 

| Property name    | Optional | Description                                                  |
| ---------------- | -------- | ------------------------------------------------------------ |
| server           | false    | The broker address of the mqtt server, such as ``tcp://127.0.0.1:1883`` |
| topic            | false    | The mqtt topic, such as ``analysis/result``                  |
| clientId         | true     | The client id for mqtt connection. If not specified, an uuid will be used |
| protocol_version | true     | 3.1 (also refer as MQTT 3) or 3.1.1 (also refer as MQTT 4).  If not specified, the default value is 3.1. |
| username         | true     | The user name for the connection.                        |
| password         | true     | The password for the connection.                             |

Below is one of the sample configuration.
```json
{
  "mqtt": {
  	"server": "tcp://sink_server:1883",
  	"topic": "demoSink",
  	"clientId": "client_id_1",
    "protocol_version": "3.1.1"
  }
}
```


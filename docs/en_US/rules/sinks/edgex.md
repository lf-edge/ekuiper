# EdgeX Message Bus action

The action is used for publish output message into EdgeX message bus.

| Property name | Optional | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| protocol      | true     | If it's not specified, then use default value ``tcp``.       |
| host          | true     | The host of message bus. If not specified, then use default value ``*``. |
| port          | true     | The port of message bus. If not specified, then use default value ``5563``. |
| topic         | false    | The topic to be published. The property must be specified.   |
| contentType   | true     | The content type of message to be published. If not specified, then use the default value ``application/json``. |

Below is sample configuration for publish result message to ``applicaton`` topic of EdgeX Message Bus.
```json
	{
      "edgex": {
        "protocol": "tcp",
        "host": "*",
        "port": 5563,
        "topic": "application",
        "contentType": "application/json"
      }
  }
```


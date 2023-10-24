# Websocket action

The action is used for publishing output message into websocket channel.

## Properties

| Property name  | Optional | Description                                           |
| addr           | false    | The address of the websocket sink server, like: 127.0.0.1:8080 |
| path           | false    | The url path of the websocket sink server, like: /api/data     |

Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.

## Sample usage

The following is an example of publishing compressed data to a websocket server.

```json
{
  "websocket":{
    "address": "127.0.0.1:8080",
    "path": "/api/data"
  }
}
```
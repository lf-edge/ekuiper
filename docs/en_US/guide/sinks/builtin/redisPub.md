# RedisPub action

The action is used for publishing output message into redis channel.

## Properties

| Property name  | Optional | Description                                           |
|----------------|----------|-------------------------------------------------------|
| address        | false    | The address of Redis, e.g., 127.0.0.1:6379                        |
| username       | true     | Redis login username (fill in if authentication is required)                         |
| password       | true     | Redis login password (fill in if authentication is required)               |
| db             | false    | The Redis database, e.g., 0        |
| channel        | false    | Specifies the Redis channels to subscribe to.     |
| compression    | true     | Compresses the Payload using the specified compression method. Currently supports zlib, gzip, flate, zstd algorithms.|

Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.

## Sample usage

The following is an example of publishing compressed data to a local Redis server.

```json
{
  "redis":{
    "address": "127.0.0.1:6379",
    "username": "default",
    "password": "123456",
    "db": 0,
    "channel": "exampleChannel",
    "compression": "zlib"
  }
}
```

This example configuration is used to publish data to the "exampleChannel" channel in Redis and applies zlib compression.

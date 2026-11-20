# gRPC Sink

The sink calls a unary method of a [gRPC](https://grpc.io/) service for each result. The result is encoded as the
protobuf request message of the method by the sink [format](../../serialization/serialization.md#format), and the
response of the call is discarded.

## Compile the plugin

The sink is built into the full version of eKuiper. To use it as a plugin, run the below command in the eKuiper source
code root path:

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Grpc.so extensions/sinks/grpc/grpc.go
```

Then copy `plugins/sinks/Grpc.so` to the `plugins/sinks` folder of the eKuiper installation and restart the eKuiper
server to activate the plugin.

## Properties

| Property name      | Optional | Description                                                                                                                                                                                             |
|--------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| server             | false    | The address of the gRPC server in the `host:port` format, such as `127.0.0.1:50051`.                                                                                                                    |
| method             | false    | The full name of the unary method to call in the `package.Service/Method` form, such as `sensor.Collector/Report`. A leading `/` is allowed. It supports [dynamic properties](../overview.md#dynamic-properties). |
| timeout            | true     | The timeout to connect to the server and for each call. It can be an integer in milliseconds or a duration string such as `10s`. The default value is `5000`.                                           |
| metadata           | true     | The [metadata](https://grpc.io/docs/guides/metadata/) of each call as a map of string values. The values support [dynamic properties](../overview.md#dynamic-properties).                              |
| insecureSkipVerify | true     | Whether to skip the verification of the server certificate. Setting it to `true` enables TLS.                                                                                                           |
| certificationPath  | true     | The client certificate file path for TLS. Setting it enables TLS.                                                                                                                                       |
| privateKeyPath     | true     | The client private key file path for TLS. Setting it enables TLS.                                                                                                                                       |
| rootCaPath         | true     | The CA certificate file path to verify the server. Setting it enables TLS.                                                                                                                              |
| certificationRaw   | true     | The base64 encoded client certificate. `certificationPath` takes precedence if both are defined.                                                                                                        |
| privateKeyRaw      | true     | The base64 encoded client private key. `privateKeyPath` takes precedence if both are defined.                                                                                                           |
| rootCARaw          | true     | The base64 encoded CA certificate. `rootCaPath` takes precedence if both are defined.                                                                                                                   |

Without any TLS property, the connection is in plaintext. Other common sink properties are supported. Please refer to
the [sink common properties](../overview.md#common-properties) for more information.

For security, connections to loopback or private network addresses (such as `127.0.0.1`) are blocked by default. To
connect to such addresses, set `basic.enablePrivateNet: true` in `etc/kuiper.yaml`.
See [global configurations](../../../configuration/global_configurations.md) for details.

You can check the connectivity of the server in advance through the API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check).

## Encode the request

The sink sends the encoded bytes of each result as the request message, so the following common properties are
required:

- `format`: must be `protobuf`, or `custom` with a format plugin that encodes protobuf messages
  like the [static protobuf](../../serialization/serialization.md#static-protobuf).
- `schemaId`: the request message type in the `schemaName.MessageName` form. The schema must be registered in
  the [schema registry](../../serialization/serialization.md#schema-registry) first.
- `sendSingle`: must be `true` so that each row is encoded as a request message. Batching by `batchSize`
  or `lingerInterval` is not supported.

The protobuf format looks up the message by its name without the package. If the `.proto` file of the service declares
a `package`, register a schema with the request message only and without the `package` declaration. The package does not
change the encoding of the message, and it is only required in the `method`.

## Delivery guarantee

The client connects to the server when the rule starts and reconnects automatically if the connection is lost. The
connection status is reported in the rule status. If the server is not available, the rule keeps running and the calls
fail until the server is back.

If a call fails with the `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` or `ABORTED` status, the error is an IO
error, so the result is sent again if the [cache](../overview.md#caching) or the resend is configured. Other errors,
such as `INVALID_ARGUMENT` or `UNIMPLEMENTED`, will not succeed if the same request is sent again, so they are not
retried by the resend interval. The errors are reported in the rule status.

## Sample usage

Assume a server implements the below service:

```protobuf
syntax = "proto3";

package sensor;

service Collector {
  rpc Report (Reading) returns (Ack) {}
}

message Reading {
  string deviceId = 1;
  double temperature = 2;
}

message Ack {
  bool ok = 1;
}
```

Register the request message as the schema `sensor` through the [schema registry API](../../../api/restapi/schemas.md):

```shell
POST http://{{host}}/schemas/protobuf
Content-Type: application/json

{
  "name": "sensor",
  "content": "syntax = \"proto3\";\nmessage Reading {\n  string deviceId = 1;\n  double temperature = 2;\n}\n"
}
```

Then call the `Report` method for each row:

```json
{
  "id": "grpcReport",
  "sql": "SELECT deviceId, temperature FROM demo WHERE temperature > 30",
  "actions": [
    {
      "grpc": {
        "server": "127.0.0.1:50051",
        "method": "sensor.Collector/Report",
        "metadata": {
          "authorization": "Bearer token"
        },
        "format": "protobuf",
        "schemaId": "sensor.Reading",
        "sendSingle": true
      }
    }
  ]
}
```

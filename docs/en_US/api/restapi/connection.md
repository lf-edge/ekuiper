# Manage connection

## Connection management

Source/Sink in rules can be created and managed independently in the form of connections.

### Create connection

To create a connection, provide the connection's id, type, and configuration parameters. Currently, `mqtt`/`nng`/`httppush`/`websocket`/`edgex`/`sql` type connections are supported. Here we take creating an mqtt connection as an example.

```shell
POST http://localhost:9081/connections
{
  "id": "connecton-1",
  "typ":"mqtt",
  "props": {
    "server": "tcp://127.0.0.1:1883"
  }
}
```

### Update connection

To update a connection, provide the connection's id, type, and configuration parameters. Currently, `mqtt`/`nng`/`httppush`/`websocket`/`edgex`/`sql` types of connections are supported. Here we take updating the mqtt connection as an example. If the connection is referenced by a rule, it cannot be updated.

```shell
PUT http://localhost:9081/connections/connection-1
{
  "id": "connecton-1",
  "typ":"mqtt",
  "props": {
    "server": "tcp://127.0.0.1:1883"
  }
}
```

### Get all connection information

```shell
GET http://localhost:9081/connections
```

Return all connections' information and status.

### Get a single connection status

```shell
GET http://localhost:9081/connections/{id}
```

### Delete a single connection

When deleting a connection, it will check whether there are rules using the connection. If there are rules using the connection, the connection cannot be deleted.

```shell
DELETE http://localhost:9081/connections/{id}
```

## Connectivity check

Check eKuiper connection connectivity via API

### sink connection check

```shell
POST http://localhost:9081/metadata/sinks/connection/{sink}
{
  "configuration": "xxxx"
}
```

The sink-side connection check will check the connectivity of the connection based on the incoming sinkType and configuration. Take mysql Sink as an example:

```shell
POST http://localhost:9081/metadata/sinks/connection/sql
{
  "url": "mysql://root@127.0.0.1:4000/test",
  "table": "test",
  "fields": ["a","b","c"]
}
```

### Source side connection check

```shell
POST http://localhost:9081/metadata/sources/connection/{source}
{
  "configuration": "xxxx"
}
```

The source-side connection check will check the connectivity of the connection based on the incoming sourceType and configuration. Take mysql Source as an example:

```shell
POST http://localhost:9081/metadata/sources/connection/sql
{
  "url": "mysql://root@127.0.0.1:4000/test",
}
```

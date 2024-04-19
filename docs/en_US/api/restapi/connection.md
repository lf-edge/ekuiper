# Manage connection

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

## Manage websocket connection

Manage websocket endpoint connection in eKuiper through REST API

## create websocket endpoint

```shell
POST http://localhost:9081/connection/websocket
```

example：

```json
{
  "endpoint": "/xxxx"
}
```

## delete websocket endpoint

```shell
DELETE http://localhost:9081/connection/websocket
```

example：

```json
{
  "endpoint": "/xxxx"
}
```

## query websocket endpoint

```shell
GET http://localhost:9081/connection/websocket
```

example：

```json
{
  "endpoint": "/xxxx"
}
```

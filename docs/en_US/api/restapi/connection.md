# Manage websocket connection

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

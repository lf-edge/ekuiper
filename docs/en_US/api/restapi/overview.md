# API Reference

eKuiper provides a set of REST API for streams and rules management in addition to CLI.

By default, the REST API are running in port 9081. You can change the port in `/etc/kuiper.yaml` for the `restPort` property.

## Getting information

This API is used to get the version number, system type, program running time, and service status.

```shell
GET http://localhost:9081
```

Response when running normally:

```json
{
  "version": "2.1.0",
  "os": "linux",
  "upTimeSeconds": 14,
  "status": "running",
  "degraded": false
}
```

Response when running in degraded mode (primary database failed):

```json
{
  "version": "2.1.0",
  "os": "linux",
  "upTimeSeconds": 14,
  "status": "degraded",
  "degraded": true,
  "degradedReason": "file is not a database (26)",
  "reason": "file is not a database (26)"
}
```

## ping

```shell
GET http://localhost:9081/ping
```

## Degraded Mode and Storage Fault Handling

When eKuiper starts up, it automatically performs a fast integrity check on the SQLite databases using `PRAGMA quick_check(1)`.

- **Primary Database Failure**: If the primary database (`sqliteKV.db`) fails to initialize (e.g., corruption or I/O error), eKuiper does not discard user data. Instead, it enters **degraded mode** to keep the REST management API available for inspection, diagnostics, and recovery.
- **Auxiliary Database Failure**: If an auxiliary database (`checkpoint.db`, `cache.db`, `extState.db`, or `trace.db`) fails to initialize, eKuiper handles it in a **best-effort** manner: the primary service continues starting up in normal mode (`status: "running"`), preserving existing database files on disk without automatic deletion. Unaffected functions and rules continue running normally, while features depending on the faulted auxiliary database safely return errors.

In degraded mode:
- Stream and rule processing, background components, and automatic rule recovery are bypassed to prevent crashes.
- Only safe maintenance and health check endpoints are available: `GET /`, `GET /ping`, `POST /reset`, and `POST /stop` (or `GET /stop`).
- Other API endpoints return HTTP `503 Service Unavailable` with an error message indicating that eKuiper is running in degraded mode and advising to call `POST /reset`.
- The administrator can either repair the database and restart eKuiper, or call `POST /reset` to clean runtime persistent data and reinitialize the system.

## Batch request

This API is used to merge multiple requests into one request and send it for execution

```shell
POST http://localhost:9081/batch/req

[
    {
        "method": "POST",
        "path": "/streams",
        "body": "{\"sql\":\"CREATE stream demobatch() WITH (DATASOURCE=\\\"/data1\\\", TYPE=\\\"websocket\\\")\"}"
    },
    {
        "method": "GET",
        "path": "/streams/demobatch"
    }
]

Response

[
    {
        "code": 201,
        "response": "Stream demobatch is created."
    },
    {
        "code": 200,
        "response": "{\"Name\":\"demobatch\",\"StreamFields\":null,\"Options\":{\"datasource\":\"/data1\",\"type\":\"websocket\"},\"StreamType\":0,\"Statement\":null}"
    }
]
```

- [Streams](streams.md)
- [Rules](rules.md)
- [Plugins](plugins.md)
- [Configs & Operations](configs.md)

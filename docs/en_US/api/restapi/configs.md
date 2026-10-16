# Dynamic Reload Configs

By dynamically reloading [configuration](../../configuration/global_configurations.md), parameters such as debug and timezone
can be updated for running eKuiper without restarting the application.

## Reload Basic Configs

```shell
PATCH http://localhost:9081/configs
```

Request demo:

```json
{
  "debug": true,
  "consoleLog": true,
  "fileLog": true,
  "timezone": "UTC"
}
```

Current supported dynamic reloadable parameters:

- `debug`
- `consoleLog`
- `fileLog`
- `timezone`

## Shutdown eKuiper

```shell
POST http://localhost:9081/stop
```

Shut down eKuiper through rest api.

## Reset eKuiper

```shell
POST http://localhost:9081/reset
```

> [!WARNING]
> `/reset` is a destructive administrative operation that permanently wipes runtime persistent state. Ensure that [REST API Authentication](authentication.md) is enabled in production environments to prevent unauthorized reset requests.

Query parameters:

- `force` (bool, optional, default `false`): Required in normal mode (`force=true`) to prevent accidental reset. In degraded mode, this parameter is optional.

Factory reset eKuiper runtime persistent data and restart the service for clean factory reinitialization:

- **Runtime persistent data cleanup**: Clears all runtime persistent data files and directories in the `data` directory (including SQLite databases `sqliteKV.db`, `cache.db`, `checkpoint.db`, `extState.db`, `trace.db`, etc.), while preserving initial configuration files such as `data/init.json` (and `etc/init.json`).
- **Initialization marker reset**: Deletes `initialized*` marker files in `etc` to allow `init.json` configurations to be reapplied upon restart.
- **Storage backend requirement**: Currently supported for the SQLite storage backend (`store.type: sqlite`). Calling `/reset` with an unsupported storage backend returns HTTP `400 Bad Request`.

Modes:

- **Degraded Mode**: Allows direct reset without parameters (`POST http://localhost:9081/reset`).
- **Normal Mode**: Requires `?force=true` (`POST http://localhost:9081/reset?force=true`). Calling without `force=true` returns HTTP `400 Bad Request`. When accepted, new operations are rejected, running rules and background components are safely stopped, store connections are closed, runtime data is cleaned up, and the process exits for container or systemd restart.
- **Duplicate Prevention**: If a reset is already in progress, subsequent `/reset` requests return HTTP `409 Conflict`.

Response (HTTP 202 Accepted):

```json
{
  "message": "reset process initiated, eKuiper is resetting and restarting..."
}
```

The eKuiper process sends this response, completes orderly shutdown and runtime data cleanup, and exits. The external service manager (e.g., systemd or container runtime) will restart eKuiper to perform clean factory initialization based on initial configurations.

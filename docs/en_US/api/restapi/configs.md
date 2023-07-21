# Dynamic Reload Configs

By dynamically reloading configuration, parameters such as debug and timezone
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

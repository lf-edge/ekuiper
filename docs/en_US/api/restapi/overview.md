eKuiper provides a set of REST API for streams and rules management in addition to CLI.

By default, the REST API are running in port 9081. You can change the port in `/etc/kuiper.yaml` for the `restPort` property.

## Getting information

This API is used to get the version number, system type, and program running time.

```shell
GET http://localhost:9081
```

```json
{
"version": "1.0.1-22-g119ee91",
"os": "darwin",
"upTimeSeconds": 14
}
```

## ping

```shell
GET http://localhost:9081/ping
```

- [Streams](streams.md)
- [Rules](rules.md)
- [Plugins](plugins.md)

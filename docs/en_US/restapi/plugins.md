# Plugins management

The Kuiper REST api for plugins allows you to manage plugins, such as create, drop and list plugins. Notice that, drop a plugin will need to restart kuiper to take effect. To update a plugin, do the following:
1. Drop the plugin.
2. Restart Kuiper.
3. Create the plugin with the new configuration.

## create a plugin

The API accepts a JSON content to create a new plugin. Each plugin type has a standalone endpoint. The supported types are `["sources", "sinks", "functions"`. The plugin is identified by the name. The name must be unique.
```shell
POST http://localhost:9081/plugins/sources
POST http://localhost:9081/plugins/sinks
POST http://localhost:9081/plugins/functions
```
Request Sample

```json
{
  "name":"random",
  "file":"http://127.0.0.1/plugins/sources/random.zip",
  "callback":"http://mycallback.com"
}
```

### Parameters

1. name: a unique name of the plugin. The name must be the same as the camel case version of the plugin with lowercase first letter. For example, if the exported plugin name is `Random`, then the name of this plugin is `random`.
2. file: the url of the plugin files. It must be a zip file with: a compiled so file and the yaml file(only required for sources). The name of the files must match the name of the plugin. Please check [Extension](../extension/overview.md) for the naming rule.
3. callback: optional parameter to specify the url to call once the plugin is created. We will issue a GET request to the callback url.


## show plugins

The API is used for displaying all of plugins defined in the server for a plugin type.

```shell
GET http://localhost:9081/plugins/sources
GET http://localhost:9081/plugins/sinks
GET http://localhost:9081/plugins/functions
```

Response Sample:

```json
["plugin1","plugin2"]
```

## drop a rule

The API is used for drop the plugin. The kuiper server needs to be restarted to take effect.

```shell
DELETE http://localhost:8080/rules/sources/{name}
DELETE http://localhost:8080/rules/sinks/{name}
DELETE http://localhost:8080/rules/functions/{name}
```
The user can pass a query parameter for a callback url. Kuiper will issue a GET request to the callback url after deleting the plugin.
```shell
DELETE http://localhost:8080/rules/sources/{name}?callback=http%3A%2F%2Fwww.mycallback.com%2Fcallback
```
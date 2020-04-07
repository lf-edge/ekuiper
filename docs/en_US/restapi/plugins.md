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
  "file":"http://127.0.0.1/plugins/sources/random.zip"
}
```

### Parameters

1. name: a unique name of the plugin. The name must be the same as the camel case version of the plugin with lowercase first letter. For example, if the exported plugin name is `Random`, then the name of this plugin is `random`.
2. file: the url of the plugin files. It must be a zip file with: a compiled so file and the yaml file(only required for sources). The name of the files must match the name of the plugin. Please check [Extension](../extension/overview.md) for the naming rule.


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

## describe a plugin

The API is used to print out the detailed definition of a plugin.

```shell
GET http://localhost:9081/plugins/sources/{name}
GET http://localhost:9081/plugins/sinks/{name}
GET http://localhost:9081/plugins/functions/{name}
```

Path parameter `name` is the name of the plugin.

Response Sample: 

```json
{
  "name": "plugin1",
  "version": "1.0.0"
}
```

## drop a plugin

The API is used for drop the plugin. The kuiper server needs to be restarted to take effect.

```shell
DELETE http://localhost:8080/plugins/sources/{name}
DELETE http://localhost:8080/plugins/sinks/{name}
DELETE http://localhost:8080/plugins/functions/{name}
```
The user can pass a query parameter to decide if Kuiper should be stopped after a delete in order to make the deletion take effect. The parameter is `restart` and only when the value is `1` will the Kuiper be stopped. The user has to manually restart it.
```shell
DELETE http://localhost:8080/plugins/sources/{name}?restart=1
```
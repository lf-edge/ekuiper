
The eKuiper REST api for plugins allows you to manage plugins, such as create, drop and list plugins. Notice that, drop a plugin will need to restart eKuiper to take effect. To update a plugin, do the following:
1. Drop the plugin.
2. Restart eKuiper.
3. Create the plugin with the new configuration.

## create a plugin

The API accepts a JSON content to create a new plugin. Each plugin type has a standalone endpoint. The supported types are `["sources", "sinks", "functions","portables"]`. The plugin is identified by the name. The name must be unique.
```shell
POST http://localhost:9081/plugins/sources
POST http://localhost:9081/plugins/sinks
POST http://localhost:9081/plugins/functions
POST http://localhost:9081/plugins/portables
```
Request Sample when the file locates in a http server

```json
{
  "name":"random",
  "file":"http://127.0.0.1/plugins/sources/random.zip"
}
```

Request Sample for files locates in the same machine of the Kuiepr server.

```json
{
  "name":"random",
  "file":"file:///var/plugins/sources/random.zip"
}
```

### Parameters

1. name: a unique name of the plugin. The name must be the same as the camel case version of the plugin with lowercase first letter. For example, if the exported plugin name is `Random`, then the name of this plugin is `random`.
2. file: the url of the plugin files. The url can be `http` or `https` scheme or `file` scheme to refer to a local file path of the eKuiper server. It must be a zip file with: a compiled so file and the yaml file(only required for sources). If the plugin depends on some external dependencies, a bash script named install.sh can be provided to do the dependency installation. The name of the files must match the name of the plugin. Please check [Extension](../../extension/overview.md) for the naming rule.

### Plugin File Format
`Note`: For `portables` type, please refer to this [format](../../extension/portable/overview.md#package).

A sample zip file for a source named random.zip
1. Random@v1.0.0.so
2. random.yaml
3. install.sh
4. Various dependency files/folders of install.sh   
   - mysdk.zip
   - myconfig.conf
5. etc directory: the runtime configuration files or dependency files. After installation, this directory will be renamed to the plugin name under {{eKuiperPath}}/etc/{{pluginType}} directory.

Notice that, the install.sh will be run that the system may already had the lib or package. Make sure to check the path before. Below is an example install.sh to install a sample sdk lib. 
```bash
#!/bin/sh
dir=/usr/local/mysdk
cur=$(dirname "$0")
echo "Base path $cur" 
if [ -d "$dir" ]; then
    echo "SDK path $dir exists." 
else
    echo "Creating SDK path $dir"
    mkdir -p $dir
    echo "Created SDK path $dir"
fi

apt install --no-upgrade unzip
if [ -d "$dir/lib" ]; then
    echo "SDK lib path $dir/lib exists." 
else
    echo "Unzip SDK lib to path $dir"
    unzip $cur/mysdk.zip -d $dir
    echo "Unzipped SDK lib to path $dir"
fi

if [ -f "/etc/ld.so.conf.d/myconfig.conf" ]; then
    echo "/etc/ld.so.conf.d/myconfig.conf exists"
else
    echo "Copy conf file"
    cp $cur/myconfig.conf /etc/ld.so.conf.d/
    echo "Copied conf file"
fi
ldconfig
echo "Done"
```

## show plugins

The API is used for displaying all of plugins defined in the server for a plugin type.

```shell
GET http://localhost:9081/plugins/sources
GET http://localhost:9081/plugins/sinks
GET http://localhost:9081/plugins/functions
GET http://localhost:9081/plugins/portables
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
GET http://localhost:9081/plugins/portables/{name}
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

The API is used for drop the plugin. The eKuiper server needs to be restarted to take effect.

```shell
DELETE http://localhost:9081/plugins/sources/{name}
DELETE http://localhost:9081/plugins/sinks/{name}
DELETE http://localhost:9081/plugins/functions/{name}
DELETE http://localhost:9081/plugins/portables/{name}
```
The user can pass a query parameter to decide if eKuiper should be stopped after a delete in order to make the deletion take effect. The parameter is `stop` and only when the value is `1` will the eKuiper be stopped. The user has to manually restart it.
```shell
DELETE http://localhost:9081/plugins/sources/{name}?stop=1
```

## APIs to handle function plugin with multiple functions

Unlike source and sink plugins, function plugin can export multiple functions at once. The exported names must be unique globally across all plugins. There will be a one to many mapping between function and its container plugin. Thus, we provide show udf(user defined function) api to query all user defined functions so that users can check the name duplication. And we provide describe udf api to find out the defined plugin of a function. We also provide the register functions api to register the udf list for an auto loaded plugin.

### show udfs

The API is used for displaying all user defined functions which are defined across all plugins.

```shell
GET http://localhost:9081/plugins/udfs
```

Response Sample:

```json
["func1","func2"]
```

### describe an udf

The API is used to find out the plugin which defines the UDF.

```shell
GET http://localhost:9081/plugins/udfs/{name}
```

Response Sample:

```json
{
  "name": "funcName",
  "plugin": "pluginName"
}
```

### register functions

The API aims to register all exported functions in an auto loaded function plugin or when the exported functions are changed. If the plugin was loaded by CLI create command or REST create API with functions property specified, then this is not needed. The register API will persist the functions list in the kv. Unless the exported functions are changed, users only need to register it once.

```shell
POST http://{{host}}/plugins/functions/{plugin_name}/register

{"functions":["func1","func2"]}

```

## Get the available plugins

According to the configuration `pluginHosts` in file `etc/kuiper.yaml` ,  it returns the plugins list that can be installed at local run eKuiper instance. By default, it get the list from `https://packages.emqx.io` .

```
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
```

The sample result is as following, and the key is plugin name, the value is plugin download address.

```json
{
  "file": "http://127.0.0.1:63767/kuiper-plugins/0.9.1/sinks/alpine/file_arm64.zip",
  "influx": "http://127.0.0.1:63767/kuiper-plugins/0.9.1/sinks/alpine/influx_arm64.zip",
  "zmq": "http://127.0.0.1:63768/kuiper-plugins/0.9.1/sinks/alpine/zmq_arm64.zip"
}
```
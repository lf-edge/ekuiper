# Plugins management

The eKuiper plugin command line tools allows you to manage plugins, such as create, show and drop plugins. Notice that, drop a plugin will need to restart eKuiper to take effect. To update a plugin, do the following:
1. Drop the plugin.
2. Restart eKuiper.
3. Create the plugin with the new configuration.

## create a plugin

The command is used for creating a plugin.  The plugin's definition is specified with JSON format.

```shell
create plugin $plugin_type $plugin_name $plugin_json | create plugin $plugin_type $plugin_name -f $plugin_def_file
```

The plugin can be created with two ways. 

- Specify the plugin definition in command line.

Sample:

```shell
# bin/kuiper create plugin source random {"file":"http://127.0.0.1/plugins/sources/random.zip"}
```

The command create a source plugin named ``random``. 

- Specify the plugin definition in a file. If the plugin is complex, or the plugin is already wrote in text files with well organized formats, you can just specify the plugin definition through ``-f`` option.

Sample:

```shell
# bin/kuiper create plugin sink plugin1 -f /tmp/plugin1.txt
```

Below is the contents of ``plugin1.txt``.

```json
{
  "file":"http://127.0.0.1/plugins/sources/random.zip"
}
```

To create a function plugin with multiple exported functions, specify the exported functions list as below:

```shell
# bin/kuiper create plugin function mulfuncs "{\"file\":\"file:///tmp/kuiper/plugins/functions/mulfuncs.zip\",\"functions\":[\"func1\",\"func2\"]}"}
```

### parameters
1. plugin_type: the type of the plugin. Available values are `["source", "sink", "functions"]`
2. plugin_name: a unique name of the plugin. The name must be the same as the camel case version of the plugin with lowercase first letter. For example, if the exported plugin name is `Random`, then the name of this plugin is `random`.
3. file: the url of the plugin files. It must be a zip file with: a compiled so file and the yaml file(only required for sources). The name of the files must match the name of the plugin. Please check [Extension](../extension/overview.md) for the naming rule.
4. functions: only apply to function plugin which exports multiple functions. The property specifies the exported function names.

## show plugins

The command is used for displaying all plugins defined in the server for a plugin type.

```shell
show plugins function
```

Sample:

```shell
# bin/kuiper show plugins function
function1
function2
```

## describe a plugin
The command is used to print out the detailed definition of a plugin.

```shell
describe plugin $plugin_type $plugin_name
```

Sample: 

```shell
# bin/kuiper describe plugin source plugin1
{
  "name": "plugin1",
  "version": "1.0.0"
}
```

## drop a plugin

The command is used for drop the plugin.

```shell
drop plugin $plugin_type $plugin_name -s $stop 
```
In which, `-s $stop` is an optional boolean parameter. If it is set to true, the eKuiper server will be stopped for the delete to take effect. The user will need to restart it manually.
Sample:

```shell
# bin/kuiper drop plugin source random
Plugin random is dropped.
```

## commands to handle function plugin with multiple functions

Unlike source and sink plugins, function plugin can export multiple functions at once. The exported names must be unique globally across all plugins. There will be a one to many mapping between function and its container plugin. Thus, we provide show udf(user defined function) command to query all user defined functions so that users can check the name duplication. And we provide udf describe command to find out the defined plugin of a function. We also provide the function register command to register the udf list for an auto loaded plugin.

### show udfs

The command will list all user defined functions. 

```shell
show udfs
```

### describe an udf

The command will show the plugin which defines the udf.

```shell
describe udf $udf_name
```

Sample output:

```json
{
  "name": "funcName",
  "plugin": "pluginName"
}
```

### register functions

The command aims to register all exported functions in an auto loaded function plugin or when the exported functions are changed. If the plugin was loaded by create command or REST create API with functions property specified, then this is not needed. The register command will persist the functions list in the kv. Unless the exported functions are changed, users only need to register it once.

```shell
register plugin function $pluginName "{\"functions\":[\"$funcName\",\"$anotherFuncName\"]}"
```

Sample:
```shell
# bin/kuiper register plugin function myPlugin "{\"functions\":[\"func1\",\"func2\",\"funcn\"]}"
```
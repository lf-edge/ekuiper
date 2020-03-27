# Plugins management

The Kuiper plugin command line tools allows you to manage plugins, such as create, show and drop plugins. Notice that, drop a plugin will need to restart kuiper to take effect. To update a plugin, do the following:
1. Drop the plugin.
2. Restart Kuiper.
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
# bin/cli create plugin source random {"file":"http://127.0.0.1/plugins/sources/random.zip"}
```

The command create a source plugin named ``random``. 

- Specify the plugin definition in a file. If the plugin is complex, or the plugin is already wrote in text files with well organized formats, you can just specify the plugin definition through ``-f`` option.

Sample:

```shell
# bin/cli create plugin sink plugin1 -f /tmp/plugin1.txt
```

Below is the contents of ``plugin1.txt``.

```json
{
  "file":"http://127.0.0.1/plugins/sources/random.zip"
}
```
### parameters
1. plugin_type: the type of the plugin. Available values are `["source", "sink", "functions"]`
2. plugin_name: a unique name of the plugin. The name must be the same as the camel case version of the plugin with lowercase first letter. For example, if the exported plugin name is `Random`, then the name of this plugin is `random`.
3. file: the url of the plugin files. It must be a zip file with: a compiled so file and the yaml file(only required for sources). The name of the files must match the name of the plugin. Please check [Extension](../extension/overview.md) for the naming rule.

## show plugins

The command is used for displaying all plugins defined in the server for a plugin type.

```shell
show plugins function
```

Sample:

```shell
# bin/cli show plugins function
function1
function2
```

## drop a plugin

The command is used for drop the plugin.

```shell
drop plugin $plugin_type $plugin_name -r $restart 
```
In which, `-r $restart` is an optional boolean parameter. If it is set to true, the Kuiper server will be stopped for the delete to take effect. The user will need to restart it manually.
Sample:

```shell
# bin/cli drop plugin source random
Plugin random is dropped.
```
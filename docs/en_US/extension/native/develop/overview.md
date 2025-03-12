# Native Plugin Development

Users can utilize the Go language native plugin system to write Source, Sink, and function implementations using Go.
Regardless of the type of plugin being developed, the following steps are required:

1. Create a plugin project.
2. Write the plugin's implementation logic according to the type of extension.
3. Build the plugin so.
4. Package the plugin so and any dependent files such as metadata/configuration files into a plugin zip package.

## Setup the plugin developing environment

It is required to build the plugin with exactly the same version of dependencies
especially `github.com/lf-edge/ekuiper/contract/v2`. Users can manage the plugin project independently, ensuring that
the Go language version in `go.mod` and the versions of the dependent modules are consistent with those of the main
project.

For example, when developing a plugin for the eKuiper v2.0.0 version, you need to first check the `go.mod` file
corresponding to the eKuiper version. Ensure that the Go version and the contract mod version in the plugin project are
consistent. For instance, in the following plugin `go.mod`, the contract mod v2.0.0 version and Go 1.24.0 version are
used.

```go.mod
module mycompany.com/myplugin

require github.com/lf-edge/ekuiper/contract/v2 v2.0.0

go 1.24.0
```

### Plugin development

The development of plugins is to implement a specific interface according to the plugin type and export the
implementation with a specific name. There are two types of exported symbol supported:

1. Export a constructor function: Kuiper will use the constructor function to create a new instance of the plugin
   implementation for each load. So each rule will have one instance of the plugin, and each instance will be isolated
   from others. This is the recommended way.

    ```go
    func Random() api.Source {
        return random.GetSource()
    }
    ```

2. Export an instance: eKuiper will use the instance as singleton for all plugin loads. So all rules will share the same
   instance. For such implementation, the developer will need to handle the shared states to avoid any potential
   multi-thread problems. This mode is recommended where there are no shared states and the performance is critical.
   Especially, a function extension is usually functional without internal state which is suitable for this mode.

    ```go
      var Random = random.GetSource()
    ```

Implementing extensions for data sources (source), data sinks (sink), and functions (function) requires different
interfaces. For more details, please refer to:

- [Source Interface](./source.md)
- [Sink Interface](./sink.md)
- [Function Interface](./function.md)

## State storage

eKuiper extension exposes a key value state storage interface through the context parameter, which can be used for all
types of extensions, including Source/Sink/Function extensions.

States are key-value pairs, where the key is a string, and the value is arbitrary data. Keys are scoped to the current
extended instance.

Users can access the state storage through the context object. State-related methods include putState, getState,
incrCounter, getCounter and deleteState.

Below is an example of a function extension to access states. This function will count the number of words passed in and
save the cumulative number in the state.

```go
func (f *accumulateWordCountFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
logger := ctx.GetLogger()
err := ctx.IncrCounter("allwordcount", len(strings.Split(args[0], args[1])))
if err != nil {
return err, false
}
if c, err := ctx.GetCounter("allwordcount"); err != nil   {
return err, false
} else {
return c, true
}
}
```

## Runtime dependencies

Some plugins may need to access dependencies in the file system. Those files are put under
<span v-pre>{{eKuiperPath}}/etc/{{pluginType}}/{{pluginName}}</span> directory. When packaging the plugin, put those
files
in [etc directory](../../../api/restapi/plugins.md#plugin-file-format). After installation, they will be moved to the
recommended place.

In the plugin source code, developers can access the dependencies of file system by getting the eKuiper root path from
the context:

```go
ctx.GetRootPath()
```

## Plugin Compilation

After completing the plugin code, users need to use the Go language compilation tool to compile the plugin so file for
the corresponding environment. **Note** that the plugin must be compiled using the same compilation environment as the
main project eKuiper.

- User-compiled eKuiper main program: The plugin can be compiled in the main program's compilation environment. This
  scenario is common during plugin development.
- Precompiled eKuiper binary or default Docker image: These versions of eKuiper are compiled using the alpine docker
  image. The specific version can be checked by viewing the corresponding version's Dockerfile source code (
  deploy/docker/Dockerfile). The plugin should be compiled using the same version of the docker image.
- eKuiper -slim or -slim-python Docker image: These versions of eKuiper are compiled using the debian docker image. The
  specific version can be checked by viewing the corresponding version's Dockerfile source code (
  deploy/docker/Dockerfile-slim). The plugin should be compiled using the same version of the docker image.

After preparing the environment, the following compilation command can be used:

```bash
go build -trimpath --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
```

### Naming

We recommend plugin name to be camel case. Notice that there are some restrictions for the names:

1. The name of the export symbol of the plugin should be camel case with an **upper case first letter**. It must be the
   same as the plugin name except the first letter. For example, plugin name _file_ must export an export symbol name
   _File_ .
2. The name of _.so_ file must be the same as the export symbol name or the plugin name. For example, _MySource.so_ or
   _mySink.so_.

### Version

The user can **optionally** add a version string to the name of _.so_ to help identify the version of the plugin. The
version can be then retrieved through describe CLI command or REST API. The naming convention is to add a version string
to the name after _@_. The version can be any string. If the version string starts with "v", the "v" will be ignored in
the return result. Below are some typical examples.

- _MySource@v1.0.0.so_ : version is 1.0.0
- _MySource@20200331.so_:  version is 20200331

If multiple versions of plugins with the same name in place, only the latest version(ordered by the version string) will
be taken effect.

## Plugin Packaging

After the plugin is compiled, the resulting so file, the default configuration file xx.yaml (required for source
plugins), the plugin description file xx.json, and any other files that the plugin depends on must all be packaged into
a zip file. There are no special requirements for the zip file name; users can name it themselves. **Note**: All files
must be at the root directory of the zip, and there should be no additional folders.

## Further Reading

The process of developing and packaging plugins can be cumbersome. You can follow
the [Plugin Tutorial](./plugins_tutorial.md) step by step to complete the plugin writing and deployment.

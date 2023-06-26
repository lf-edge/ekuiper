# Features

Except core runtime and REST api, there are some features that are allowed to be enabled or disabled during compilation by [go build constraints](https://pkg.go.dev/go/build#hdr-Build_Constraints). Uses can customize the built binary to include only the desired features to reduce the binary size according to the limit of the target environment.

## Feature List

| Feature                                                                                           | Build Tag  | Description                                                                                                                                            |
|---------------------------------------------------------------------------------------------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| Core                                                                                              | core       | The core of eKuiper. It contains the processor and REST API for stream/table/rule, the configuration processing, the SQL parser, the rule runtime etc. |
| [CLI](../../api/cli/overview.md)                                                                  | rpc        | The cli server                                                                                                                                         |
| [EdgeX Foundry integration](../../edgex/edgex_rule_engine_tutorial.md)                            | edgex      | The built-in edgeX source, sink and connection                                                                                                         |
| [Native plugin](../../extension/native/overview.md)                                               | plugin     | The native plugin runtime, REST API, CLI API etc.                                                                                                      |
| [Portable plugin](../../extension/portable/overview.md)                                           | portable   | The portable plugin runtime, REST API, CLI API etc.                                                                                                    |
| [External service](../../extension/external/external_func.md)                                     | service    | The external service runtime, REST API, CLI API etc.                                                                                                   |
| [Msgpack-rpc External service](../../extension/external/external_func.md)                         | msgpack    | Support msgpack-rpc protocol in external service                                                                                                       |
| [UI Meta API](../../operation/manager-ui/overview.md)                                             | ui         | The REST API of the metadata which is usually consumed by the ui                                                                                       |
| [Prometheus Metrics](../../configuration/global_configurations.md#prometheus-configuration)       | prometheus | Support to send metrics to prometheus                                                                                                                  |
| [Extended template functions](../../guide/sinks/data_template.md#functions-supported-in-template) | template   | Support additional data template function from sprig besides default go text/template functions                                                        |
| [Codecs with schema](../../guide/serialization/serialization.md)                                  | schema     | Support schema registry and codecs with schema such as protobuf                                                                                        |

## Usage

In makefile, we already provide three feature sets: standard, edgeX and core. The standard feature set include all features in the list except edgeX; edgeX feature set include all features; And the core feature set is the minimal which only has core feature. Build these feature sets with default makefile:

```shell
# standard
make
# EdgeX
make build_with_edgex
# core
make build_core
```

Feature selection is useful in a limited resource target which is unlikely to run as docker container. So we only provide standard feature set in the docker images.

And users need to build from source to customize the feature sets. To build with the desired features:

```shell
go build --tags "<FEATURE>"
```

For example, to build with core and native plugin support:

```shell
go build --tags "core plugin"
```

Recommend updating the build command in the Makefile with tags and build from make.

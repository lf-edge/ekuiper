# Installation

eKuiper provides docker image, binary packages and helm chart to install.

## Running eKuiper in Docker

Please make sure docker has installed before running.

1. Get docker image.

   ```shell
   docker pull lfedge/ekuiper:x.x.x
   ```

2. Start docker container.

   ```shell
   docker run -p 9081:9081 -d --name kuiper -e MQTT_SOURCE__DEFAULT__SERVER=tcp://broker.emqx.io:1883 lfedge/ekuiper:xxx
   ```

In this example, we specify the default MQTT broker via environment variable to `broker.emqx.io`, which is a public MQTT test server hosted by [EMQ](https://www.emqx.io).

For more configuration and docker image tags, please check [lfedge/ekuiper in docker hub](https://hub.docker.com/r/lfedge/ekuiper).

## Running eKuiper with management console

eKuiper manager is a free eKuiper management web console which is provided as a docker image. We can use docker compose to run both eKuiper and eKuiper manager at once to ease the usage.

Please make sure docker compose has installed before running.

1. Create `docker-compose.yaml` file.

   ```yaml
   version: '3.4'

   services:
       manager:
          image: emqx/ekuiper-manager:x.x.x
          container_name: ekuiper-manager
          ports:
          - "9082:9082"
          restart: unless-stopped
          environment:
            # setting default eKuiper service, works since 1.8.0
            DEFAULT_EKUIPER_ENDPOINT: "http://ekuiper:9081"
       ekuiper:
          image: lfedge/ekuiper:x.x.x
          ports:
            - "9081:9081"
            - "127.0.0.1:20498:20498"
          container_name: ekuiper
          hostname: ekuiper
          restart: unless-stopped
          user: root
          volumes:
            - /tmp/data:/kuiper/data
            - /tmp/log:/kuiper/log
          environment:
            MQTT_SOURCE__DEFAULT__SERVER: "tcp://broker.emqx.io:1883"
            KUIPER__BASIC__CONSOLELOG: "true"
            KUIPER__BASIC__IGNORECASE: "false"
     ```

2. Start docker-compose cluster.

   ```shell
   $ docker-compose -p my_ekuiper up -d
   ```

3. Check docker images running status, make sure two containers are started.

   ```shell
   $ docker ps
   CONTAINER ID   IMAGE                         COMMAND                  CREATED              STATUS                  PORTS                                                NAMES
   e2dbcd4c1f92   lfedge/ekuiper:latest          "/usr/bin/docker-ent…"   7 seconds ago        Up Less than a second   0.0.0.0:9081->9081/tcp, 127.0.0.1:20498->20498/tcp   ekuiper
   fa7c33b3e114   emqx/ekuiper-manager:latest   "/usr/bin/docker-ent…"   About a minute ago   Up 59 seconds           0.0.0.0:9082->9082/tcp                               manager
   ```

Please check [use of eKuiper management console](./operation/manager-ui/overview.md) to set up and configure the eKuiper manager.

## Install From Zip

eKuiper binary packages are released on below operating systems with AMD64, ARM and ARM64 support:

- Raspbian 10
- Debian 9
- Debian 10
- Ubuntu 16.04
- Ubuntu 18.04
- Ubuntu 20.04
- macOS

For other operating systems such as Windows, users can [compile from source code manually](#compile-from-source-code).

1. Download eKuiper zip or tar for your CPU architecture from [ekuiper.org](https://ekuiper.org/downloads) or [Github](https://github.com/lf-edge/ekuiper/releases).
2. Unzip the installation file:

    ```shell
    unzip kuiper-x.x.x-linux-amd64.zip
    ```

3. Start eKuiper.

    ```shell
    $ bin/kuiperd
    ```

4. Remove eKuiper. Simply delete the eKuiper directory.

After installation, all the files are inside the unzipped directory. Please check [installed directory structure](#installation-structure) for detail.

## Install from package

1. Download eKuiper package for your CPU architecture from [ekuiper.org](https://ekuiper.org/downloads) or [Github](https://github.com/lf-edge/ekuiper/releases).
2. Install eKuiper.
   - DEB package:

     ```shell
     # for debian/ubuntu
     $ sudo apt install ./kuiper-x.x.x-linux-amd64.deb
     ```

3. Start eKuiper.
   - quick start

     ```shell
     $ sudo kuiperd
     ```

   - systemctl

     ```shell
     sudo systemctl start kuiper
     ```

4. Remove eKuiper.
   - DEB:

     ```shell
     sudo apt remove --purge kuiper
     ```

When installing by package, the eKuiper folders are not in the same directory. The installation structure is as below:

```text
/usr/lib/kuiper/bin
  kuiperd
  kuiper
/etc/kuiper
  ...
/var/lib/kuiper/data
  ...
/var/lib/kuiper/plugins
  ...
/var/log/kuiper
   ...
```

## Install via Helm (K8S、K3S)

The eKuiper Helm chart is published to the GitHub Container Registry (OCI).

1. Install eKuiper directly from OCI registry.

   ```shell
   $ helm install my-ekuiper oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper --version 1.4.0
   ```

   Or pull the chart first:

   ```shell
   $ helm pull oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper --version 1.4.0
   $ helm install my-ekuiper ./ekuiper-1.4.0.tgz
   ```

2. View eKuiper status.

   ```shell
   $ kubectl get pods
   NAME         READY  STATUS    RESTARTS  AGE
   my-ekuiper-0 1/1    Running   0         56s
   ```

3. Customize values (optional).

   ```shell
   # Show default values
   $ helm show values oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper --version 1.4.0

   # Install with custom values
   $ helm install my-ekuiper oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper --version 1.4.0 \
       --set persistence.enabled=true \
       --set service.type=LoadBalancer
   ```


## Compile from source code

1. Get the source code.

   ```shell
   $ git clone https://github.com/lf-edge/ekuiper.git
   ```

2. Compile.

   ```shell
   $ make
   ```

3. Start eKuiper.

   ```shell
   $ cd _build/kuiper-x.x.x-linux-amd64/
   $ bin/kuiperd
   ```

### Compile Packages and Docker Images

- Packages: `$ make pkg`
  - Packages files that support EdgeX: `$ make pkg_with_edgex`
- Docker images: `$ make docker`

  > Notice that: Docker images support EdgeX by default

### Cross-compile binaries

Go supports cross-compiling binaries for multiple platforms which applies to eKuiper as well. Because eKuiper depends on
sqlite, CGO_ENABLED must be set to 1 which requires to install and specify the gcc of the target system.

- Install the GNU toolchain/gcc of the target system.
- Modify the Makefile to specify `GOOS`, `GOARCH` and `CC`  and then build.

For example, to cross build ARM64 binaries in AMD64 ubuntu/debian machine, do these steps:

1. Install the GNU toolchain/gcc of the target system ARM64

      ```shell
      apt-get install gcc-aarch64-linux-gnu
      ```

2. Update the Makefile in the build command. Examples:

      ```shell
      GO111MODULE=on CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc go build -trimpath -ldflags="-s -w -X github.com/lf-edge/ekuiper/cmd.Version=$(VERSION) -X github.com/lf-edge/ekuiper/cmd.LoadFileType=relative" -o kuiperd cmd/kuiperd/main.go
      ```

3. Run `make`

### Compile with Selected Features

eKuiper allows tailoring the binary in compilation to get a customized feature set.
As written by go, it also allows cross compilation.
Except core runtime and REST api,
there are some features
that are allowed
to be enabled or disabled during compilation
by [go build constraints](https://pkg.go.dev/go/build#hdr-Build_Constraints).
Uses can customize the built binary
to include only the desired features to reduce the binary size according to the limit of the target environment.

| Feature                                                                                       | Build Tag  | Description                                                                                                                                            |
|-----------------------------------------------------------------------------------------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| Core                                                                                          | core       | The core of eKuiper. It contains the processor and REST API for stream/table/rule, the configuration processing, the SQL parser, the rule runtime etc. |
| [CLI](./api/cli/overview.md)                                                                  | rpc        | The cli server                                                                                                                                         |
| [EdgeX Foundry integration](./edgex/edgex_rule_engine_tutorial.md)                            | edgex      | The built-in edgeX source, sink and connection                                                                                                         |
| [Native plugin](./extension/native/overview.md)                                               | plugin     | The native plugin runtime, REST API, CLI API etc.                                                                                                      |
| [Portable plugin](./extension/portable/overview.md)                                           | portable   | The portable plugin runtime, REST API, CLI API etc.                                                                                                    |
| [External service](./extension/external/external_func.md)                                     | service    | The external service runtime, REST API, CLI API etc.                                                                                                   |
| [Msgpack-rpc External service](./extension/external/external_func.md)                         | msgpack    | Support msgpack-rpc protocol in external service                                                                                                       |
| [UI Meta API](./operation/manager-ui/overview.md)                                             | ui         | The REST API of the metadata which is usually consumed by the ui                                                                                       |
| [Prometheus Metrics](./configuration/global_configurations.md#prometheus-configuration)       | prometheus | Support to send metrics to prometheus                                                                                                                  |
| [Extended template functions](./guide/sinks/data_template.md#functions-supported-in-template) | template   | Support additional data template function from sprig besides default go text/template functions                                                        |
| [Codecs with schema](./guide/serialization/serialization.md)                                  | schema     | Support schema registry and codecs with schema such as protobuf                                                                                        |

In makefile, we already provide three feature sets: standard, edgeX and core. The standard feature set include all
features in the list except edgeX; edgeX feature set include all features; And the core feature set is the minimal which
only has core feature. Build these feature sets with default makefile:

```shell
# standard
make
# EdgeX
make build_with_edgex
# core
make build_core
```

Feature selection is useful in a limited resource target which is unlikely to run as docker container. So we only
provide standard feature set in the docker images.

And users need to build from source to customize the feature sets. To build with the desired features:

```shell
go build --tags "<FEATURE>"
```

For example, to build with core and native plugin support:

```shell
go build --tags "core plugin"
```

Recommend updating the build command in the Makefile with tags and building from make.

## Installation structure

Below is the directory structure after installation.

```text
bin
  kuiperd
  kuiper
etc
  ...
data
  ...
plugins
  ...
log
  ...
```

### bin

The `bin` directory includes all of executable files. Such as the kuiper server `kuiperd` and the cli client `kuiper`.

### etc

The `etc` directory contains the default configuration files of eKuiper. Such as the global configuration file `kuiper.yaml` and all the source configuration files such as `mqtt_source.yaml`.

### data

This folder saves the persisted definitions of streams and rules. It also contains any user defined configurations.

### plugins

eKuiper allows users to develop your own plugins, and put these plugins into this folder.  See [extension](./extension/overview.md) for more info for how to extend the eKuiper.

### log

All the log files are under this folder. The default log file name is `stream.log`.

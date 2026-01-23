# 安装

eKuiper 提供了 docker 镜像、二进制包和 Helm Chart 等安装方式。

## 在 Docker 中运行

运行前请确认 Docker 已经安装。

1. 获取 Docker 镜像。

   ```shell
   docker pull lfedge/ekuiper:x.x.x
   ```

2. 启动 Docker 容器。

   ```shell
   docker run -p 9081:9081 -d --name kuiper -e MQTT_SOURCE__DEFAULT__SERVER=tcp://broker.emqx.io:1883 lfedge/ekuiper:xxx
   ```

在这个例子中，我们通过环境变量指定默认的 MQTT 代理为`broker.emqx.io`，这是一个由 [EMQ](https://www.emqx.io) 托管的公共 MQTT 测试服务器。

更多 eKuiper 镜像配置和标签说明，请查看 [docker hub](https://hub.docker.com/r/lfedge/ekuiper)。

## 使用管理控制台运行

eKuiper manager 是一个免费的 eKuiper 管理控制台，以 Docker 镜像的形式提供。我们可以使用 docker compose 来一键运行 eKuiper 和 eKuiper manager。

在运行之前，请确保 docker compose 已经安装。

1. 创建 `docker-compose.yaml` 文件。

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

2. 启动 docker-compose 集群。

   ```shell
   $ docker-compose -p my_ekuiper up -d
   ```

3. 检查 Docker 镜像的运行状态，确保两个容器正常启动。

   ```shell
   $ docker ps
   CONTAINER ID   IMAGE                         COMMAND                  CREATED              STATUS                  PORTS                                                NAMES
   e2dbcd4c1f92   lfedge/ekuiper:latest          "/usr/bin/docker-ent…"   7 seconds ago        Up Less than a second   0.0.0.0:9081->9081/tcp, 127.0.0.1:20498->20498/tcp   ekuiper
   fa7c33b3e114   emqx/ekuiper-manager:latest   "/usr/bin/docker-ent…"   About a minute ago   Up 59 seconds           0.0.0.0:9082->9082/tcp                               manager
   ```

请查看 [使用 eKuiper 管理控制台](./operation/manager-ui/overview.md) 来设置和配置 eKuiper 管理控制台。

## 通过 Zip 包安装

eKuiper 发布了以下操作系统的二进制包，支持 AMD64、ARM 和 ARM64 等 CPU 架构。

- Raspbian 10
- Debian 9
- Debian 10
- Ubuntu 16.04
- Ubuntu 18.04
- Ubuntu 20.04
- macOS

对于其他操作系统，如 Windows，用户可以 [从源代码手动编译](#从源码编译)。

1. 从 [ekuiper.org](https://ekuiper.org/downloads) 或 [Github](https://github.com/lf-edge/ekuiper/releases) 下载适合你 CPU 架构的 eKuiper zip 或 tar 包。
2. 解压安装包：

    ```shell
    unzip kuiper-x.x.x-linux-amd64.zip
    ```

3. 启动 eKuiper.

    ```shell
    $ bin/kuiperd
    ```

4. 卸载 eKuiper：删除 eKuiper 文件夹即可。

安装后，所有的文件都在未压缩的目录内。请查看 [安装的目录结构](#目录结构)了解详情。

## 通过软件包安装

1. 从 [ekuiper.org](https://ekuiper.org/downloads) 或 [Github](https://github.com/lf-edge/ekuiper/releases) 下载适合你 CPU 架构的 eKuiper 软件包。
2. 安装 eKuiper.
   - DEB 包：

     ```shell
     # for debian/ubuntu
     $ sudo apt install ./kuiper-x.x.x-linux-amd64.deb
     ```

3. 启动 eKuiper.
   - 快速启动

     ```shell
     $ sudo kuiperd
     ```

   - systemctl

     ```shell
     sudo systemctl start kuiper
     ```

4. 移除 eKuiper.
   - DEB:

     ```shell
     sudo apt remove --purge kuiper
     ```

当按软件包安装时，eKuiper 的文件夹不在同一个目录中。安装后的目录结构如下。

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

## 通过 Helm 安装（K8S、K3S）

eKuiper Helm chart 发布在 GitHub Container Registry (OCI) 上。

1. 直接从 OCI registry 安装 eKuiper。

   ```shell
   $ helm install my-ekuiper oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper --version 1.4.0
   ```

   或者先拉取 chart：

   ```shell
   $ helm pull oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper --version 1.4.0
   $ helm install my-ekuiper ./ekuiper-1.4.0.tgz
   ```

2. 查看 eKuiper 状态。

   ```shell
   $ kubectl get pods
   NAME         READY  STATUS    RESTARTS  AGE
   my-ekuiper-0 1/1    Running   0         56s
   ```

3. 自定义配置（可选）。

   ```shell
   # 查看默认配置
   $ helm show values oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper --version 1.4.0

   # 使用自定义配置安装
   $ helm install my-ekuiper oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper --version 1.4.0 \
       --set persistence.enabled=true \
       --set service.type=LoadBalancer
   ```

## 从源码编译

1. 获取源代码

   ```shell
   $ git clone https://github.com/lf-edge/ekuiper.git
   ```

2. 编译

   ```shell
   $ make
   ```

3. 启动 eKuiper

   ```shell
   $ cd _build/kuiper-x.x.x-linux-amd64/
   $ bin/kuiperd
   ```

### 编译打包和 Docker 镜像

- 安装文件打包：
  - 安装文件打包：: `$ make pkg`
  - 支持 EdgeX 的安装文件打包: `$ make pkg_with_edgex`
- Docker 镜像：`$ make docker`
  > 请注意，Docker 镜像默认支持 EdgeX

### 交叉编译二进制文件

Go 语言支持交叉编译多种目标平台的二进制文件。eKuiper 项目也支持标准的 Go 语言交叉编译。由于 eKuiper 依赖
sqlite，因此 `CGO_ENABLE` 必须设置为1。在交叉编译时，必须安装核指定目标系统的 gcc 工具链。

- 安装目标系统 gcc 工具链。
- 修改 Makefile 添加 `GOOS`, `GOARCH` 和 `CC`  编译参数，并编译。

例如，在 AMD64 架构的 ubuntu/debian 系统中，可使用下列步骤编译针对 ARM64 架构的 linux 系统的二进制包。

1. 安装 ARM64 的 gcc 工具链。

      ```shell
      apt-get install gcc-aarch64-linux-gnu
      ```

2. 更新 Makefile 里的编译相关参数如下:

      ```shell
      GO111MODULE=on CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc go build -trimpath -ldflags="-s -w -X github.com/lf-edge/ekuiper/cmd.Version=$(VERSION) -X github.com/lf-edge/ekuiper/cmd.LoadFileType=relative" -o kuiperd cmd/kuiperd/main.go
      ```

3. 运行 `make` 。

### 按需编译功能

eKuiper 允许在编译中对二进制文件进行定制，以获得定制的功能集。除了核心运行时和 REST API
，其他功能都可通过 [go build constraints](https://pkg.go.dev/go/build#hdr-Build_Constraints)
在编译时打开或者关闭。用户可编译自定义的，仅包含所需功能的二进制包从而减少包的大小，以便能够部署在资源敏感的环境中。

| 功能                                                                      | Build Tag  | 描述                                                           |
|-------------------------------------------------------------------------|------------|--------------------------------------------------------------|
| 核心                                                                      | core       | eKuiper 的核心运行时。 包括流/表/规则的处理器和 REST API ，配置管理，SQL 解析器，规则运行时等。 |
| [CLI](./api/cli/overview.md)                                            | rpc        | CLI 服务端                                                      |
| [EdgeX Foundry 整合](./edgex/edgex_rule_engine_tutorial.md)               | edgex      | 内置的 edgeX source, sink 和共享连接支持                               |
| [原生插件](./extension/native/overview.md)                                  | plugin     | 原生插件运行时，REST API和CLI API等                                    |
| [Portable 插件](./extension/portable/overview.md)                         | plugin     | Portable 插件运行时，REST API和CLI API等                             |
| [外部服务](./extension/external/external_func.md)                           | service    | 外部服务运行时，REST API和CLI API等                                    |
| [UI 元数据API](./operation/manager-ui/overview.md)                         | ui         | 元数据的 REST API，通常由 UI 端消费                                     |
| [Prometheus 指标](./configuration/global_configurations.md#prometheus-配置) | prometheus | 支持发送指标到 prometheus 中                                         |
| [扩展模板函数](./guide/sinks/data_template.md#模版中支持的函数)                       | template   | 支持除 go 语言默认的模板函数之外的扩展函数，主要来自 sprig                           |
| [有模式编解码](./guide/serialization/serialization.md)                        | schema     | 支持模式注册及有模式的编解码格式，例如 protobuf                                 |

Makefile 里已经提供了三种功能集合：标准，edgeX和核心。标准功能集合包含除了 EdgeX 之外的所有功能。edgeX
功能集合包含了所有的功能；而核心功能集合近包含最小的核心功能。可以通过以下命令，分别编译这三种功能集合：

```shell
# 标准
make
# EdgeX
make build_with_edgex
# 核心
make build_core
```

功能选择通常应用在资源受限的目标环境中。而该环境一般不太适合运行 docker 容易。因此，我们仅提供包含标准及 edgeX 功能集合的
docker 镜像。

若需要自定义功能选择，用户需要自行编译源码。其语法为：

```shell
go build --tags "<FEATURE>"
```

例如，编译带有原生插件功能的核心包，编译命令为：

```shell
go build --tags "core plugin"
```

建议用户以默认 Makefile 为模板，在里面更新编译命令，使其选择所需的 tags ，然后采用 make 命令进行编译。

## 目录结构

下面是安装后的目录结构。

```shell
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

`bin` 目录包括所有的可执行文件。例如，ekuiper 服务器 `kuiperd` 和 cli 客户端 `kuiper`。

### etc

`etc` 目录包含 eKuiper 的默认配置文件。如全局配置文件 `kuiper.yaml` 和所有源配置文件，如 `mqtt_source.yaml`。

### data

这个文件夹保存了流和规则的持久定义。它还包含任何用户定义的配置。

### plugin

eKuiper 允许用户开发你自己的插件，并将这些插件放入这个文件夹。关于如何扩展 eKuiper，请参见 [extension](./extension/overview.md)，了解更多信息。

### log

所有的日志文件都在这个文件夹下。默认的日志文件名是 `stream.log`。

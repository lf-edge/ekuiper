# 安装

eKuiper 提供了 docker镜像、二进制包和 Helm Chart 等安装方式。

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

在这个例子中，我们通过环境变量指定默认的MQTT代理为`broker.emqx.io`，这是一个由[EMQ]（https://www.emqx.io）托管的公共MQTT测试服务器。

更多 eKuiper 镜像配置和标签说明，请查看[docker hub](https://hub.docker.com/r/lfedge/ekuiper)。

## 使用管理控制台运行

eKuiper manager 是一个免费的 eKuiper 管理控制台，以 Docker 镜像的形式提供。我们可以使用docker compose 来一键运行 eKuiper 和 eKuiper manager。

在运行之前，请确保docker compose已经安装。

1. 创建 `docker-compose.yaml` 文件.
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
3. 检查 Docker镜像的运行状态，确保两个容器正常启动。
   ```shell
   $ docker ps
   CONTAINER ID   IMAGE                         COMMAND                  CREATED              STATUS                  PORTS                                                NAMES
   e2dbcd4c1f92   lfedge/ekuiper:latest          "/usr/bin/docker-ent…"   7 seconds ago        Up Less than a second   0.0.0.0:9081->9081/tcp, 127.0.0.1:20498->20498/tcp   ekuiper
   fa7c33b3e114   emqx/ekuiper-manager:latest   "/usr/bin/docker-ent…"   About a minute ago   Up 59 seconds           0.0.0.0:9082->9082/tcp                               manager
   ```

请查看[使用 eKuiper 管理控制台](./operation/manager-ui/overview.md)来设置和配置 eKuiper 管理控制台。

## 通过 Zip 包安装

eKuiper 发布了以下操作系统的二进制包，支持 AMD64、ARM 和 ARM64 等 CPU 架构。

- CentOS 7 (EL7)
- CentOS 8 (EL8)
- Raspbian 10
- Debian 9
- Debian 10
- Ubuntu 16.04
- Ubuntu 18.04
- Ubuntu 20.04
- macOS

对于其他操作系统，如Windows，用户可以[从源代码手动编译](#从源码编译)。

1.  从 [ekuiper.org](https://ekuiper.org/downloads) 或 [Github](https://github.com/lf-edge/ekuiper/releases) 下载适合你 CPU 架构的 eKuiper zip 或 tar 包。
2. 解压安装包:
    ```shell
    unzip kuiper-x.x.x-linux-amd64.zip
    ```
3. 启动 eKuiper.
    ```shell
    $ bin/kuiperd
    ```
4. 卸载 eKuiper：删除 eKuiper 文件夹即可。

安装后，所有的文件都在未压缩的目录内。请查看[安装的目录结构](#目录结构)了解详情。
    

## 通过软件包安装

1.  从 [ekuiper.org](https://ekuiper.org/downloads) 或 [Github](https://github.com/lf-edge/ekuiper/releases) 下载适合你 CPU 架构的 eKuiper 软件包。
2. 安装 eKuiper.
   - DEB 包:
     ```shell
     # for debian/ubuntu
     $ sudo apt install ./kuiper-x.x.x-linux-amd64.deb
     ```   
   - RPM 包:
     ```shell
     # for CentOS
     $ sudo rpm -ivh kuiper-x.x.x-linux-amd64.rpm
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
   - RPM:
     ```shell
     sudo yum remove kuiper
     ```

当按软件包安装时，eKuiper 的文件夹不在同一个目录中。安装后的目录结构如下。

```
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

1. 添加 helm 库。
   ```shell
    $ helm repo add emqx https://repos.emqx.io/charts
    $ helm repo update
   ```
2. 查询 eKuiper
   ```shell
    $ helm search repo emqx
    NAME         CHART VERSION APP VERSION DESCRIPTION
    emqx/emqx    v4.0.0        v4.0.0      A Helm chart for EMQX
    emqx/emqx-ee v4.0.0        v4.0.0      A Helm chart for EMQX
    emqx/ekuiper  0.1.1         0.1.1       A lightweight IoT edge analytic software
   ```
3. 启动 eKuiper
   ```shell
    $ helm install my-ekuiper emqx/ekuiper
   ``` 
4. 查看 eKuiper 状态
   ```shell
   $ kubectl get pods
   NAME         READY  STATUS    RESTARTS  AGE
   my-ekuiper-0 1/1    Running   0         56s
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

eKuiper 允许在编译中对二进制文件进行定制，以获得定制的功能集。它也允许交叉编译，详情请查看 [compilation](./operation/compile/compile.md)。

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

`etc` 目录包含 eKuiper 的默认配置文件。如全局配置文件 `kuiper.yaml` 和所有源配置文件，如`mqtt_source.yaml`。

### data

这个文件夹保存了流和规则的持久定义。它还包含任何用户定义的配置。

### plugin

eKuiper 允许用户开发你自己的插件，并将这些插件放入这个文件夹。关于如何扩展eKuiper，请参见[extension](./extension/overview.md)，了解更多信息。

### log

所有的日志文件都在这个文件夹下。默认的日志文件名是`stream.log`。
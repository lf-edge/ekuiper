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
        - /tmp/plugins:/kuiper/plugins
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

- CentOS 7 (EL7)
- CentOS 8 (EL8)
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
   - RPM package:
     ```shell
     # for CentOS
     $ sudo rpm -ivh kuiper-x.x.x-linux-amd64.rpm
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
   - RPM:
     ```shell
     sudo yum remove kuiper
     ```
     
When installing by package, the eKuiper folders are not in the same directory. The installation structure is as below:

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

## Install via Helm (K8S、K3S)

1. Add helm repository.
   ```shell
    $ helm repo add emqx https://repos.emqx.io/charts
    $ helm repo update
   ```
2. Query eKuiper.
   ```shell
    $ helm search repo emqx
    NAME         CHART VERSION APP VERSION DESCRIPTION
    emqx/emqx    v4.0.0        v4.0.0      A Helm chart for EMQX
    emqx/emqx-ee v4.0.0        v4.0.0      A Helm chart for EMQX
    emqx/ekuiper  0.1.1         0.1.1       A lightweight IoT edge analytic software
   ```
3. Start eKuiper.
   ```shell
    $ helm install my-ekuiper emqx/ekuiper
   ``` 
4. View eKuiper status.
   ```shell
   $ kubectl get pods
   NAME         READY  STATUS    RESTARTS  AGE
   my-ekuiper-0 1/1    Running   0         56s
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
   
eKuiper allows to tailor the binary in compilation to get a customized feature set. As written by go, it also allows cross compilation. For detail, please check [compilation](./operation/compile/compile.md).

## Installation structure

Below is the directory structure after installation.

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

The `bin` directory includes all of executable files. Such as the kuiper server `kuiperd` and the cli client `kuiper`.

### etc

The `etc` directory contains the default configuration files of eKuiper. Such as the global configuration file `kuiper.yaml` and all the source configuration files such as `mqtt_source.yaml`.

### data

This folder saves the persisted definitions of streams and rules. It also contains any user defined configurations.

### plugins

eKuiper allows users to develop your own plugins, and put these plugins into this folder.  See [extension](./extension/overview.md) for more info for how to extend the eKuiper.

### log

All the log files are under this folder. The default log file name is `stream.log`.

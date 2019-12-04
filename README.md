# A lightweight IoT edge analytic software

## Highlight

A SQL based lightweight IoT analytics/streaming software running at resource constrained edge devices.
- Native run with small overhead ( ~7MB package), support Linux/Windows/Mac OS
- SQL based, easy to use
- Built-in support for MQTT source
- Extension - user can customize the rule engine
- RESTful APIs for rules management

## Document
English
- [Getting started](docs/en_US/getting_started.md) 
- [Reference guide](docs/en_US/reference.md) 

中文
- [入门教程](docs/zh_CN/getting_started.md) 
- [参考指南](docs/zh_CN/reference.md)

## Build from source code

#### Prepare

+ Go version >= 1.11

#### Build binary file

+ Build binary file

  ```shell
  $ make
  ```

+ Get the compressed files
 
  ```shell
  $ make pkg
  ```

+ Get the docker image
 
  ```shell
  $ make docker
  ```

#### Cross-build binary file

+ Prepare

    + docker version >= 19.03

    + Requires experimental mode to be enabled on the Docker CLI

+ Get the cross-build compressed file

  ```shell
  $ make cross_build
  ```

+ Get the multi-platform images and push to registry

  ```shell
  $ make cross_docker
  ```

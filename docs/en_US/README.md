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
- [Getting started](./getting_started.md) 
- [Reference guide](./reference.md) 

中文
- [入门教程](https://docs.emqx.io/kuiper/cn/getting_started.html) 
- [参考指南](https://docs.emqx.io/kuiper/cn/reference.html)

## Build from source code

#### Prepare

+ Go version >= 1.11

#### Build binary file

+ Build binary file

  ```shell
  $ make
  ```

+ Cross build binary file

  ```shell
  $ GOOS=linux GOARCH=arm make 
  ```

#### Get the compressed file

+ Get the compressed files
 
  ```
  $ make pkg
  ```

+ Get the cross-build compressed file

  ```
  $ GOOS=linux GOARCH=arm make pkg
  ```
  

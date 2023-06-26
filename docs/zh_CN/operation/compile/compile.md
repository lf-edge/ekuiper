## 从源码编译

+ Go version >= 1.13

+ 编译二进制：

  + 编译二进制文件: `$ make`

  + 编译支持 EdgeX 的二进制文件: `$ make build_with_edgex`

+ 安装文件打包：

  + 安装文件打包：: `$ make pkg`

  + 支持 EdgeX 的安装文件打包: `$ make pkg_with_edgex`

+ Docker 镜像：`$ make docker`

  > Docker 镜像默认支持 EdgeX

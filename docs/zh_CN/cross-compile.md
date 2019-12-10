## 交叉编译二进制文件

**注：Kuiper 插件基于 Golang 的方式实现，由于 Golang 本身的限制，使用了交叉编译的方式必须将编译参数 ``CGO_ENABLED`` 设置为0，而在该模式下，<u>插件将不可工作</u>。所以如果使用了 Kuiper 的插件的话，<u>不能以交叉编译的方式来生成二进制包。</u>**

- 准备
  - docker version >= 19.03
  - 启用 Docker CLI 的 experimental 模式（experimental mode）
- 交叉编译二进制文件：``$ make cross_build``
- 交叉编译跨平台镜像，并推到库中：``$ make cross_docker``




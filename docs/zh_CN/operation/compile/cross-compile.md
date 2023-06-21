## 交叉编译二进制文件

Go 语言支持交叉编译多种目标平台的二进制文件。eKuiper 项目也支持标准的 Go 语言交叉编译。由于 eKuiper 依赖 sqlite，因此 `CGO_ENABLE` 必须设置为1。在交叉编译时，必须安装核指定目标系统的 gcc 工具链。

- 安装目标系统 gcc 工具链。
- 修改 Makefile 添加 `GOOS`, `GOARCH` 和 `CC`  编译参数，并编译。

例如，在 AMD64 架构的 ubuntu/debian 系统中，可使用下列步骤编译针对 ARM64 架构的 linux 系统的二进制包。

1. 安装 ARM64 的 gcc 工具链。

      ```shell
      apt-get install gcc-aarch64-linux-gnu
      ```

2. 更新 Makefile 里的编译相关参数如下:

      ```shell
      GO111MODULE=on CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o kuiperd cmd/kuiperd/main.go
      ```

3. 运行 `make` 。

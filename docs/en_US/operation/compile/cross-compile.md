## Cross-compile binaries

Go supports cross-compiling binaries for multiple platforms which applies to eKuiper as well. Because eKuiper depends on sqlite, CGO_ENABLED must be set to 1 which requires to install and specify the gcc of the target system.

- Install the GNU toolchain/gcc of the target system.
- Modify the Makefile to specify `GOOS`, `GOARCH` and `CC`  and then build.

For example, to cross build ARM64 binaries in AMD64 ubuntu/debian machine, do these steps:

1. Install the GNU toolchain/gcc of the target system ARM64

      ```shell
      apt-get install gcc-aarch64-linux-gnu
      ```

2. Update the Makefile in the build command. Examples:

      ```shell
      GO111MODULE=on CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc go build -trimpath -ldflags="-s -w -X main.Version=$(VERSION) -X main.LoadFileType=relative" -o kuiperd cmd/kuiperd/main.go
      ```

3. Run `make`

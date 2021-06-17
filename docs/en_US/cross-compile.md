## Cross-compile binaries

**Notice: eKuiper plugins bases on Golang, and due to Golang restrictions, ``CGO_ENABLED``  flag must be set to 0 to use the Golang cross-compile. But with this flag mode, the Golang plugins will not work. So if you want to use plugins in eKuiper, you can NOT use cross-compile to produce the binary packages.**

- Preparation
  - docker version >= 19.03
  - Enable Docker CLI  experimental mode
- Cross-compile binary files: ``$ make cross_build``
- Cross-compile images for all platforms and push to registry:``$ make cross_docker``


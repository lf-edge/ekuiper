FROM golang:1.14 AS builder

COPY . /go/kuiper

WORKDIR /go/kuiper

RUN apt update && apt install -y pkg-config libczmq-dev

RUN make build_with_edgex \
    && ln -s /go/kuiper/_build/kuiper-$(git describe --tags --always)-$(uname -s | tr "[A-Z]" "[a-z]")-$(uname -m) /usr/local/kuiper 

RUN go build  -o kuiper_conf_util deploy/docker/conf_util.go \
    && if [ "$(uname -m)" != "aarch64" ]; then apt install -y upx && upx ./kuiper_conf_util; fi \
    && ln -s /go/kuiper/kuiper_conf_util /usr/bin/kuiper_conf_util

RUN ln -s /go/kuiper/deploy/docker/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

EXPOSE 9081 20498

ENV KUIPER_HOME /usr/local/kuiper
ENV KUIPER__BASIC__CONSOLELOG true

WORKDIR /usr/local/kuiper
VOLUME ["/usr/local/kuiper/etc", "/usr/local/kuiper/data", "/usr/local/kuiper/plugins", "/usr/local/kuiper/log"]
ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

CMD ["./bin/server"]

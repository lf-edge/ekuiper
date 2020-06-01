FROM golang:1.13.10 AS builder

COPY . /go/kuiper

WORKDIR /go/kuiper

RUN apt update && apt install -y upx pkg-config libczmq-dev

RUN make build_with_edgex \
    && ln -s /go/kuiper/_build/kuiper-$(git describe --tags --always)-$(uname -s | tr "[A-Z]" "[a-z]")-$(uname -m) /usr/local/kuiper 

RUN go build  -o kuiper_conf_util deploy/docker/conf_util.go \
    && upx ./kuiper_conf_util \
    && ln -s /go/kuiper/deploy/kuiper_conf_util /usr/bin/kuiper_conf_util

RUN ln -s /go/kuiper/deploy/docker/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

EXPOSE 9081 20498

ENV KUIPER_HOME /usr/local/kuiper

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

CMD ["sh", "-c", "cd /usr/local/kuiper && ./bin/server"]
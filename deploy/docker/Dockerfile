FROM golang:1.13.10-alpine AS builder

COPY . /go/kuiper

WORKDIR /go/kuiper

RUN apk add upx gcc make git libc-dev binutils-gold pkgconfig zeromq-dev && make build_with_edgex

FROM alpine:3.10

COPY --from=builder /go/kuiper/_build/kuiper-* /kuiper/
COPY ./deploy/docker/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

RUN apk add sed libzmq

WORKDIR /kuiper

EXPOSE 9081 20498

ENV KUIPER_HOME /kuiper

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

CMD ["./bin/server"]
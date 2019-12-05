FROM golang:1.13.4-alpine AS builder

COPY . /go/kuiper

WORKDIR /go/kuiper

RUN apk add upx gcc make git libc-dev binutils-gold && make 

FROM alpine:3.10

COPY --from=builder /go/kuiper/_build/kuiper-* /kuiper/
COPY ./docker/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

RUN apk add sed

WORKDIR /kuiper

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

CMD ["./bin/server"]
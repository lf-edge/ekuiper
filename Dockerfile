FROM golang:1.13.4-alpine AS builder

COPY . /go/kuiper

WORKDIR /go/kuiper

RUN apk add upx gcc make git libc-dev && make 

FROM alpine:3.10

COPY --from=builder /go/kuiper/_build/kuiper-* /kuiper/

WORKDIR /kuiper

CMD ["./bin/server"]
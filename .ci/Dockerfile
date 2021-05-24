FROM golang:1.15.1 AS builder

COPY . /go/kuiper

WORKDIR /go/kuiper

RUN apt update \
    && apt install -y zip upx pkg-config libczmq-dev build-essential debhelper

RUN make pkg
RUN .ci/test.sh

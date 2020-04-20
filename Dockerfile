FROM golang:1.13.10 AS builder

COPY . /go/kuiper

RUN apt update && apt install -y zip upx pkg-config libczmq-dev && make -C /go/kuiper pkg
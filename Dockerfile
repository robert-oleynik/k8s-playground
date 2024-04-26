ARG GO_VERSION=1.22.2
FROM golang:${GO_VERSION} AS builder
ARG TARGET

RUN mkdir /project
COPY ./storage /project/storage
COPY ./raft /project/raft
COPY ./tester /project/tester
COPY ./go.work /project

WORKDIR /project
RUN go mod download
RUN mkdir out
RUN go build -o /project/out/${TARGET} /project/${TARGET}

FROM debian:latest
ARG TARGET

COPY --from=builder /project/out/${TARGET} /usr/bin/service

ENTRYPOINT [ "/usr/bin/service" ]

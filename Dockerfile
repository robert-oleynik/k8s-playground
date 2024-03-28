ARG GO_VERSION=1.22.1

FROM golang:${GO_VERSION} AS builder
ARG TARGET

RUN mkdir /project
COPY ./storage /project/storage
COPY ./raft-test /project/raft-test
COPY ./raft /project/raft
COPY ./go.work ./go.work.sum /project
WORKDIR /project
RUN go mod download
RUN mkdir out
RUN go build -o /project/out/${TARGET} /project/${TARGET}

FROM golang:${GO_VERSION}
ARG TARGET

COPY --from=builder /project/out/${TARGET} /usr/bin/${TARGET}

ENTRYPOINT [ "/usr/bin/${TARGET}" ]

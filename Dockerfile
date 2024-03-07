ARG RUST_VERSION=1.76.0
ARG DEBIAN_VERSION=bookworm

FROM rust:${RUST_VERSION}-slim-${DEBIAN_VERSION} AS builder

ARG PACKAGE

COPY Cargo.toml /project/Cargo.toml
COPY Cargo.lock /project/Cargo.lock
COPY services /project/services

WORKDIR /project

RUN cargo build --locked --release -p ${PACKAGE}

FROM debian:${DEBIAN_VERSION}

COPY --from=builder /project/target/release/${PACKAGE} /usr/bin/${PACKAGE}

ENTRYPOINT [ "/usr/bin/${PACKAGE}" ]

#########################################################################################
## docker will invoke this file from ../../.. dir in order to access the code
#########################################################################################
ARG LOTUS_TEST_IMAGE=filecoin/lotus-test:latest
FROM ${LOTUS_TEST_IMAGE} AS lotus-test
#########################################################################################
FROM node:24-trixie AS react-builder

WORKDIR /src
COPY react /src/react
COPY gql /src/gql

RUN npm_config_legacy_peer_deps=yes npm ci --no-audit --prefix react&& \
      npm run --prefix react build
#########################################################################################
FROM rust:1.86.0-slim-bookworm AS rust-toolchain
#########################################################################################
FROM golang:1.25-trixie AS builder
LABEL Maintainer="Boost Development Team"

RUN apt-get update && apt-get install -y ca-certificates build-essential clang ocl-icd-opencl-dev ocl-icd-libopencl1 jq libhwloc-dev pkg-config gcc git hwloc mesa-opencl-icd

WORKDIR /opt/boost

ENV XDG_CACHE_HOME="/tmp"

# Copy prebuilt Rust from the rust image (arch-matched by buildx)
COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN rustc --version && cargo --version

RUN go install github.com/ipld/go-car/cmd/car@latest \
 && cp $GOPATH/bin/car /usr/local/bin/

COPY go.mod go.sum ./
COPY extern/boostd-data/go.mod extern/boostd-data/go.sum ./extern/boostd-data/
COPY extern/filecoin-ffi/go.mod extern/filecoin-ffi/go.sum ./extern/filecoin-ffi/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

RUN git submodule update --init

### make configurable filecoin-ffi build
ARG FFI_BUILD_FROM_SOURCE=0
ENV FFI_BUILD_FROM_SOURCE=${FFI_BUILD_FROM_SOURCE}

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make clean deps

COPY --from=react-builder /src/react/build /go/src/react/build

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make debug
#########################################################################################
FROM ubuntu:24.04 AS runner

RUN apt-get update && apt-get install -y dnsutils vim curl aria2 jq git wget

## Copy libs
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /lib/*/libdl.so.2 /lib/
COPY --from=builder /lib/*/librt.so.1 /lib/
COPY --from=builder /lib/*/libgcc_s.so.1 /lib/
COPY --from=builder /lib/*/libutil.so.1 /lib/
COPY --from=builder /usr/lib/*/libltdl.so.7 /lib/
COPY --from=builder /usr/lib/*/libnuma.so.1 /lib/
COPY --from=builder /usr/lib/*/libhwloc.so.* /lib/
COPY --from=builder /usr/lib/*/libOpenCL.so.1 /lib/

WORKDIR /app

COPY --from=lotus-test /usr/local/bin/lotus /usr/local/bin/
COPY --from=lotus-test /usr/local/bin/lotus-miner /usr/local/bin/
COPY --from=lotus-test /usr/local/bin/lotus-shed /usr/local/bin/
## Test lotus starts
RUN lotus -v && lotus-miner -v
#########################################################################################
FROM runner AS boost-dev

ARG BUILD_VERSION=0.1

LABEL org.opencontainers.image.version=$BUILD_VERSION \
      org.opencontainers.image.authors="Boost Dev Team" \
      name="boost-dev" \
      maintainer="Boost Dev Team" \
      vendor="Boost Dev Team" \
      version=$BUILD_VERSION \
      release=$BUILD_VERSION \
      summary="This image is used to host the boost-dev storage provider" \
      description="This image is used to host the boost-dev storage provider"

ENV BOOST_PATH /var/lib/boost
ENV BOOSTD_DATA_PATH /var/lib/boostd-data
ENV BOOST_CLIENT_REPO /var/lib/boost/boost-client
ENV LID_LEVELDB_PATH /var/lib/boost
VOLUME /var/lib/boost
EXPOSE 8080

COPY --from=builder /opt/boost/boostd /usr/local/bin/
COPY --from=builder /opt/boost/boost /usr/local/bin/
COPY --from=builder /opt/boost/boostx /usr/local/bin/
COPY --from=builder /opt/boost/boostd-data /usr/local/bin/
COPY --from=builder /usr/local/bin/car /usr/local/bin/
COPY docker/devnet/boost/entrypoint.sh /app/
COPY docker/devnet/boost/sample/* /app/sample/
RUN boost -v

ENTRYPOINT ["./entrypoint.sh"]
#########################################################################################
FROM runner AS booster-http-dev

ARG BUILD_VERSION=0.1

LABEL org.opencontainers.image.version=$BUILD_VERSION \
      org.opencontainers.image.authors="Boost Dev Team" \
      name="booster-http-dev" \
      maintainer="Boost Dev Team" \
      vendor="Boost Dev Team" \
      version=$BUILD_VERSION \
      release=$BUILD_VERSION \
      summary="This image is used to host booster-http-dev" \
      description="This image is used to host booster-http-dev"

EXPOSE 7777

COPY --from=builder /opt/boost/boostd /usr/local/bin/
COPY --from=builder /opt/boost/booster-http /usr/local/bin/
COPY docker/devnet/booster-http/entrypoint.sh /app/

ENTRYPOINT ["./entrypoint.sh"]
#########################################################################################
FROM runner AS booster-bitswap-dev

ARG BUILD_VERSION=0.1

LABEL org.opencontainers.image.version=$BUILD_VERSION \
      org.opencontainers.image.authors="Boost Dev Team" \
      name="booster-bitswap-dev" \
      maintainer="Boost Dev Team" \
      vendor="Boost Dev Team" \
      version=$BUILD_VERSION \
      release=$BUILD_VERSION \
      summary="This image is used to host booster-bitswap-dev" \
      description="This image is used to host booster-bitswap-dev"

EXPOSE 8888

COPY --from=builder /opt/boost/boostd /usr/local/bin/
COPY --from=builder /opt/boost/booster-bitswap /usr/local/bin/
COPY docker/devnet/booster-bitswap/entrypoint.sh /app/

ENTRYPOINT ["./entrypoint.sh"]

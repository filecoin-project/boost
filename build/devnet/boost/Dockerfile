#########################################################################################
#########################################################################################
ARG LOTUS_TEST_IMAGE=filecoin/lotus-test:latest
FROM ${LOTUS_TEST_IMAGE} as lotus-dev
#########################################################################################
FROM golang:1.18-bullseye as builder

RUN apt update && apt install -y \
      build-essential \
      bzr pkg-config \
      clang \
      curl \
      gcc git \
      hwloc \
      jq \
      libhwloc-dev wget \
      mesa-opencl-icd \
      ocl-icd-opencl-dev

WORKDIR /go/src/

ARG BUILD_VERSION=0.1
RUN git clone --depth 1 --branch v${BUILD_VERSION} https://github.com/filecoin-project/boost

RUN cd boost && make debug
#########################################################################################
FROM ubuntu:20.04 as runner

RUN apt update && apt install -y hwloc

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

WORKDIR /app
ENV BOOST_PATH /var/lib/boost
VOLUME /var/lib/boost
EXPOSE 8080  

COPY --from=builder /go/src/boost/boostd /usr/local/bin/
COPY --from=builder /go/src/boost/boost /usr/local/bin/
COPY --from=builder /go/src/boost/boostx /usr/local/bin/
COPY --from=lotus-dev /usr/local/bin/lotus /usr/local/bin/
COPY --from=lotus-dev /usr/local/bin/lotus-miner /usr/local/bin/
## Fix missing lib libhwloc.so.5
RUN ls -1 /lib/x86_64-linux-gnu/libhwloc.so.* | head -n 1 | xargs -n1 -I {} ln -s {} /lib/x86_64-linux-gnu/libhwloc.so.5
## Smoke test for the boost and lotus
RUN lotus -v && boost -v 

COPY entrypoint.sh /app/
COPY sample/* /app/sample/

ENTRYPOINT ["./entrypoint.sh"]

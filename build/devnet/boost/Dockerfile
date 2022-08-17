#########################################################################################
## docker will invoke this file from ../../.. dir in order to access the code
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

# copy src
COPY . /go/src

RUN make debug
#####################################################################################
FROM debian:bullseye-slim as runner

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
EXPOSE 8000  

COPY --from=builder /go/src/boostd /app/
COPY build/devnet/boost/entrypoint.sh /app/

ENTRYPOINT ["./entrypoint.sh"]

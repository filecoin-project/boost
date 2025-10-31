ARG LOTUS_TEST_IMAGE=filecoin/lotus-test:latest
#############################################################################
FROM ${LOTUS_TEST_IMAGE}

ARG BUILD_VERSION=0.1

LABEL org.opencontainers.image.version=$BUILD_VERSION \
      org.opencontainers.image.authors="Boost Dev Team" \
      name="lotus-dev" \
      maintainer="Boost Dev Team" \
      vendor="Boost Dev Team" \
      version=$BUILD_VERSION \
      release=$BUILD_VERSION \
      summary="This image is used to host the lotus dev service" \
      description="This image is used to host the lotus dev service"

EXPOSE 1234
EXPOSE 9090
ENV LOTUS_SKIP_GENESIS_CHECK=_yes_
ENV GENESIS_PATH=/var/lib/genesis
ENV SECTOR_SIZE=8388608
ENV LOTUS_FEVM_ENABLEETHRPC=true
ENV LOTUS_CHAININDEXER_ENABLEINDEXER=true

VOLUME /var/tmp/filecoin-proof-parameters
VOLUME /var/lib/genesis
VOLUME /var/lib/builtin-actors

WORKDIR /app
RUN mkdir -p /app

COPY entrypoint.sh /app

USER root

ENTRYPOINT ["./entrypoint.sh"]

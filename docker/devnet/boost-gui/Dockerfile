#########################################################################################
#########################################################################################
FROM node:16.16-alpine3.15 AS builder

ARG BUILD_VERSION=0.1
WORKDIR /src/react

COPY react /src/react
COPY gql /src/gql

#TODO remove force after fixing npm dependencies 
RUN npm ci --force 

RUN npm run build
#####################################################################################
FROM nginx:1.23-alpine

ARG BUILD_VERSION=0.1

LABEL org.opencontainers.image.version=$BUILD_VERSION \
      org.opencontainers.image.authors="Boost Dev Team" \
      name="boost-gui" \
      maintainer="Boost Dev Team" \
      vendor="Boost Dev Team" \
      version=$BUILD_VERSION \
      release=$BUILD_VERSION \
      summary="This image is used to host the boost-gui service" \
      description="This image is used to host the boost-gui service" 

EXPOSE 8000
ENV BOOST_URL=http://boost:8080

COPY --from=builder /src/react/build usr/share/nginx/html
COPY docker/devnet/boost-gui/nginx.conf.in /app/nginx.conf.in
COPY docker/devnet/boost-gui/entrypoint.sh /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]

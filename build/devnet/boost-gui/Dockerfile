#########################################################################################
#########################################################################################
FROM node:16.16-alpine3.15 AS builder

RUN apk --no-cache --update add git

ARG BUILD_VERSION=0.1
WORKDIR /src
RUN git clone --depth 1 --branch v${BUILD_VERSION} https://github.com/filecoin-project/boost 

WORKDIR /src/boost/react

#TODO remove force after fixing npm dependencies 
RUN npm install --force 

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

COPY --from=builder /src/boost/react/build usr/share/nginx/html
COPY nginx.conf.in /app/nginx.conf.in
COPY entrypoint.sh /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]

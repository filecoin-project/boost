#!/usr/bin/env sh
set -e

echo Preparing config with BOOST_URL=${BOOST_URL}
cat /app/nginx.conf.in | envsubst '$BOOST_URL' > /etc/nginx/conf.d/default.conf

echo Starting nginx
exec nginx -g 'daemon off;'

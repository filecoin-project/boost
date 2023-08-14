#!/usr/bin/env bash
###################################################################################
# Generates a random, dense car file in the /app/public/ directory and issues
# a deal for it. This command assumes you have already transfered the needed funds.
# Example (from ./docker/devnet):
# $ docker compose exec boost /bin/bash
# $ export `lotus auth api-info --perm=admin`
# $ boost init
# $ lotus send --from=`lotus wallet default` `boost wallet default` 100
# $ boostx market-add 50
# $ ./sample/random-deal.sh 51200 100
###################################################################################
set -e

ci="\e[3m"
cn="\e[0m"

chunks="${1:-51200}"
links="${2:-100}"

printf "${ci}boostx generate-rand-car -c=$chunks -l=$links -s=5120000 /app/public/ | awk '{print $NF}'\n\n${cn}"

FILE=`boostx generate-rand-car -c=$chunks -l=$links -s=5120000 /app/public/ | awk '{print $NF}'`
PAYLOAD_CID=$(find "$FILE" | xargs -I{} basename {} | sed 's/\.car//')

COMMP_CID=`boostx commp $FILE 2> /dev/null | grep CID | cut -d: -f2 | xargs`
PIECE=`boostx commp $FILE 2> /dev/null | grep Piece | cut -d: -f2 | xargs`
CAR=`boostx commp $FILE 2> /dev/null | grep Car | cut -d: -f2 | xargs`

###################################################################################
printf "${ci}boost deal --verified=false --provider=t01000 \
--http-url=http://demo-http-server/$FILE \
--commp=$COMMP_CID --car-size=$CAR --piece-size=$PIECE \
--payload-cid=$PAYLOAD_CID --storage-price 20000000000\n\n${cn}"

boost deal --verified=false --provider=t01000 --http-url=http://demo-http-server/$PAYLOAD_CID.car --commp=$COMMP_CID --car-size=$CAR --piece-size=$PIECE --payload-cid=$PAYLOAD_CID --storage-price 20000000000
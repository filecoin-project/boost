#!/usr/bin/env bash
set -e

export FULLNODE_API_INFO=`lotus auth api-info --perm=admin | cut -f2 -d=`

boost init

lotus send --from=$(lotus wallet default) $(boost wallet default) 10

until boostx market-add 1; do printf "\nOops, maybe funds not added yet.\nNeed to wait some time. \n"; sleep 3; done
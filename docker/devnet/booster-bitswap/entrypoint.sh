#!/usr/bin/env bash
set -e

export FULLNODE_API_INFO=`lotus auth api-info --perm=admin | cut -f2 -d=`
export MINER_API_INFO=`lotus-miner auth api-info --perm=admin | cut -f2 -d=`
export BOOST_API_INFO=`boostd auth api-info --perm=admin | cut -f2 -d=`

echo $FULLNODE_API_INFO
echo $MINER_API_INFO
echo $BOOST_API_INFO
echo $BOOSTER_BITSWAP_REPO

if [ ! -f $BOOSTER_BITSWAP_REPO/.init.booster-bitswap ]; then
	echo Init booster-bitswap on first run ...

	booster-bitswap init

	echo Done
	touch $BOOSTER_BITSWAP_REPO/.init.booster-bitswap
fi

echo Starting booster-bitswap...
export GOLOG_LOG_LEVEL=remote-blockstore=debug,booster=debug,engine=debug,bitswap-server=debug
exec booster-bitswap --vv run --api-boost=$BOOST_API_INFO --tracing

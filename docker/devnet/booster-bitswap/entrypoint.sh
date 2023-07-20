#!/usr/bin/env bash
set -e

export FULLNODE_API_INFO=`lotus auth api-info --perm=admin | cut -f2 -d=`
export MINER_API_INFO=`lotus-miner auth api-info --perm=admin | cut -f2 -d=`

echo $FULLNODE_API_INFO
echo $MINER_API_INFO
echo $LID_API_INFO
echo $BOOSTER_BITSWAP_REPO

if [ ! -f $BOOSTER_BITSWAP_REPO/.init.booster-bitswap ]; then
	echo Init booster-bitswap on first run ...

	booster-bitswap init

	echo Done
	touch $BOOSTER_BITSWAP_REPO/.init.booster-bitswap
fi

echo Starting booster-bitswap...
export GOLOG_LOG_LEVEL=remote-blockstore=debug,booster=debug,engine=debug,bitswap-server=debug
exec booster-bitswap --vv run --api-lid=$LID_API_INFO --api-fullnode=$FULLNODE_API_INFO --api-storage=$MINER_API_INFO --tracing --engine-blockstore-worker-count=64 --engine-task-worker-count=64 --max-outstanding-bytes-per-peer=8388608 --target-message-size=524288 --task-worker-count=64

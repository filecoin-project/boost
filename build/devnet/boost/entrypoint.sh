#!/usr/bin/env bash
set -e

echo BOOST_PATH=$BOOST_PATH
if [ ! -f $BOOST_PATH/.init.boost ]; then
	echo MINER_API_INFO=$MINER_API_INFO
	echo PUBMSG_WALLET=$PUBMSG_WALLET
	echo COLLAT_WALLET=$COLLAT_WALLET
	echo FULLNODE_API_INFO=$FULLNODE_API_INFO

	echo Init boost on first run ...

	./boostd -vv --boost-repo $BOOST_PATH init --api-sealer=$MINER_API_INFO  \
		--api-sector-index=$MINER_API_INFO   \
		--wallet-publish-storage-deals=$PUBMSG_WALLET   \
		--wallet-deal-collateral=$COLLAT_WALLET   \
		--max-staging-deals-bytes=2000000000
	
	# echo exit code: $?
	
	echo SET PORT in config
	sed -i 's|ip4/0.0.0.0/tcp/0|ip4/0.0.0.0/tcp/50000|g' $BOOST_PATH/config.toml
	
	echo Done
	touch $BOOST_PATH/.init.boost
fi

echo Starting boost in dev mode...
exec ./boostd -vv run

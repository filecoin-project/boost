#!/usr/bin/env bash
set -e

echo Wait for lotus is ready ...
lotus wait-api
echo Wait for lotus-miner is ready ...
lotus-miner wait-api
echo BOOST_PATH=$BOOST_PATH
export DEFAULT_WALLET=`lotus wallet default`
export FULLNODE_API_INFO=`lotus auth api-info --perm=admin | cut -f2 -d=`
export MINER_API_INFO=`lotus-miner auth api-info --perm=admin | cut -f2 -d=`

if [ ! -f $BOOST_PATH/.init.boost ]; then
	echo Init wallets ...
    export COLLAT_WALLET=`lotus wallet new bls`
    export PUBMSG_WALLET=`lotus wallet new bls`
    export CLIENT_WALLET=`lotus wallet new bls`
	echo MINER_API_INFO=$MINER_API_INFO
	echo FULLNODE_API_INFO=$FULLNODE_API_INFO
	echo PUBMSG_WALLET=$PUBMSG_WALLET
	echo COLLAT_WALLET=$COLLAT_WALLET

    lotus send --from $DEFAULT_WALLET $COLLAT_WALLET 10
    lotus send --from $DEFAULT_WALLET $PUBMSG_WALLET 10
    lotus send --from $DEFAULT_WALLET $CLIENT_WALLET 10
    lotus wallet market add --from $DEFAULT_WALLET --address $CLIENT_WALLET 5
    lotus wallet market add --address $COLLAT_WALLET 5

	until lotus-miner actor control set --really-do-it ${PUBMSG_WALLET}; do echo Waiting for storage miner API ready ...; sleep 1; done

	echo Init boost on first run ...

	boostd -vv --boost-repo $BOOST_PATH init --api-sealer=$MINER_API_INFO  \
		--api-sector-index=$MINER_API_INFO   \
		--wallet-publish-storage-deals=$PUBMSG_WALLET   \
		--wallet-deal-collateral=$COLLAT_WALLET   \
		--max-staging-deals-bytes=2000000000

	# echo exit code: $?

	echo Setting port in boost config...
	sed -i 's|ip4/0.0.0.0/tcp/0|ip4/0.0.0.0/tcp/50000|g' $BOOST_PATH/config.toml
	sed -i 's|127.0.0.1|0.0.0.0|g' $BOOST_PATH/config.toml

	echo Done
	touch $BOOST_PATH/.init.boost
fi

# TODO(anteva): fixme: hack as boostd fails to start without this dir
mkdir -p /var/lib/boost/deal-staging

if [ ! -f $BOOST_PATH/.register.boost ]; then
	echo Temporary starting boost to get maddr...

	boostd -vv run &> $BOOST_PATH/boostd.log &
	BOOST_PID=`echo $!`
	echo Got boost PID = $BOOST_PID

	until cat $BOOST_PATH/boostd.log | grep maddr; do echo "Waiting for boost..."; sleep 1; done
	echo Looks like boost started and initialized...

	echo Registering to lotus-miner...
	MADDR=`cat $BOOST_PATH/boostd.log | grep maddr | cut -f3 -d"{" | cut -f1 -d:`
	echo Got maddr=${MADDR}

	lotus-miner actor set-peer-id ${MADDR}
	lotus-miner actor set-addrs /dns/boost/tcp/50000
	echo Registered

	touch $BOOST_PATH/.register.boost
	echo Try to stop boost...
    kill -15 $BOOST_PID || kill -9 $BOOST_PID
	rm -f $BOOST_PATH/boostd.log
	echo Super. DONE! Boostd is now configured and will be started soon
fi

echo Starting boost in dev mode...
exec boostd -vv run --nosync=true

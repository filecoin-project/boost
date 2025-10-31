#!/usr/bin/env bash
set -e

echo Wait for lotus is ready ...
lotus wait-api
echo Wait for lotus-miner is ready ...
lotus-miner wait-api
echo BOOST_PATH=$BOOST_PATH
echo BOOSTD_DATA_PATH=$BOOSTD_DATA_PATH
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
	sed 's|#ListenAddresses = \["/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"\]|ListenAddresses = \["/ip4/0.0.0.0/tcp/50000", "/ip6/::/tcp/0"\]|g' $BOOST_PATH/config.toml > $BOOST_PATH/config.toml.tmp; cp $BOOST_PATH/config.toml.tmp $BOOST_PATH/config.toml; rm $BOOST_PATH/config.toml.tmp
	sed 's|#ListenAddress = "127.0.0.1"|ListenAddress = "0.0.0.0"|g' $BOOST_PATH/config.toml > $BOOST_PATH/config.toml.tmp; cp $BOOST_PATH/config.toml.tmp $BOOST_PATH/config.toml; rm $BOOST_PATH/config.toml.tmp

  echo Setting up FIL+ wallets
  ROOT_KEY_1=`cat $LOTUS_PATH/rootkey-1`
  ROOT_KEY_2=`cat $LOTUS_PATH/rootkey-2`
  echo Root key 1: $ROOT_KEY_1
  echo Root key 2: $ROOT_KEY_2
  lotus wallet import $LOTUS_PATH/bls-$ROOT_KEY_1.keyinfo
  lotus wallet import $LOTUS_PATH/bls-$ROOT_KEY_2.keyinfo
  NOTARY_1=`lotus wallet new secp256k1`
  NOTARY_2=`lotus wallet new secp256k1`
  echo $NOTARY_1 > $BOOST_PATH/notary_1
  echo $NOTARY_2 > $BOOST_PATH/notary_2
  echo Notary 1: $NOTARY_1
  echo Notary 2: $NOTARY_2

  echo Add verifier root_key_1 notary_1
  lotus-shed verifreg add-verifier $ROOT_KEY_1 $NOTARY_1 10000000000
  sleep 15
  echo Msig inspect f080
  lotus msig inspect f080
  PARAMS=`lotus msig inspect f080 | tail -1 | awk '{print $8}'`
  echo Params: $PARAMS
  echo Msig approve
  lotus msig approve --from=$ROOT_KEY_2 f080 0 t0100 f06 0 2 $PARAMS

  echo Send 10 FIL to NOTARY_1
  lotus send $NOTARY_1 10

	echo Done
	touch $BOOST_PATH/.init.boost
fi

## Override config options
echo Updating config values
sed 's|#ServiceApiInfo = ""|ServiceApiInfo = "ws://localhost:8044"|g' $BOOST_PATH/config.toml > $BOOST_PATH/config.toml.tmp; cp $BOOST_PATH/config.toml.tmp $BOOST_PATH/config.toml; rm $BOOST_PATH/config.toml.tmp
sed 's|#ExpectedSealDuration = "24h0m0s"|ExpectedSealDuration = "0h0m10s"|g' $BOOST_PATH/config.toml > $BOOST_PATH/config.toml.tmp; cp $BOOST_PATH/config.toml.tmp $BOOST_PATH/config.toml; rm $BOOST_PATH/config.toml.tmp

## run boostd-data
#boostd-data -vv run leveldb --repo=$BOOSTD_DATA_PATH --addr=0.0.0.0:8044 &>$BOOSTD_DATA_PATH/boostd-data-ldb.log &

## run boostd-data for yugabytedb
boostd-data -vv run yugabyte --hosts yugabytedb --connect-string="postgresql://yugabyte:yugabyte@yugabytedb:5433?sslmode=disable" --addr 0.0.0.0:8044 &>$BOOSTD_DATA_PATH/boostd-data-yugabyte.log &

# TODO(anteva): fixme: hack as boostd fails to start without this dir
mkdir -p $BOOST_PATH/deal-staging

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

	curl -X POST -H "Content-Type: application/json" -d '{"query":"mutation { storageAskUpdate (update: { Price: 0, VerifiedPrice: 0 } ) }"}' http://localhost:8080/graphql/query
	echo Price SET TO 0

	touch $BOOST_PATH/.register.boost
	echo Try to stop boost...
    kill -15 $BOOST_PID || kill -9 $BOOST_PID
	rm -f $BOOST_PATH/boostd.log
	echo Super. DONE! Boostd is now configured and will be started soon
fi

echo Starting LID service and boost in dev mode...
trap 'kill %1' SIGINT
exec boostd -vv run --nosync=true --deprecated=true &>> $BOOST_PATH/boostd.log

# Devnet

To run Boost on your development machine, you will need to set up a devnet:

1. Remove any existing lotus repo and boost repo
```
rm -rf ~/.lotusmarkets && rm -rf ~/.lotus && rm -rf ~/.lotusminer && rm -rf ~/.genesis_sectors
rm -rf ~/.boost
```

2. Build lotus in debug mode

The version of lotus needs to match the version in boost's go.mod
```
cd lotus
git checkout <tag>
make debug
```

3. Install lotus

The devnet script uses the installed lotus to run the miner and daemon.
```
make install
install -C ./lotus-seed /usr/local/bin/lotus-seed
```

4. Build boost in debug mode
```
cd boost
make debug
make install
```

3. Start the devnet

This will use the lotus binaries that you built and installed above. So the lotus version must match the version in boost's go.mod 
```
cd boost
./devnet
```

4. Wait for lotus-miner to come up

Unset these variables as they interfere with the `lotus-miner` command.
```
unset MINER_API_INFO
unset FULLNODE_API_INFO
```
Then repeatedly run this command until it succeeds:
```
lotus-miner auth api-info --perm=admin
```
It should take about a minute for the miner to come up.

5. Get the auth tokens to connect to the lotus daemon and miner:
```
export ENV_MINER_API_INFO=`lotus-miner auth api-info --perm=admin`
export ENV_FULLNODE_API_INFO=`lotus auth api-info --perm=admin`

export MINER_API_INFO=`echo $ENV_MINER_API_INFO | awk '{split($0,a,"="); print a[2]}'`
export FULLNODE_API_INFO=`echo $ENV_FULLNODE_API_INFO | awk '{split($0,a,"="); print a[2]}'`

echo MINER_API_INFO=$MINER_API_INFO
echo FULLNODE_API_INFO=$FULLNODE_API_INFO
```

6. Create the wallets needed for boost
```
export DEFAULT_WALLET=`lotus wallet list | tail -1 | awk '{print $1}'`
export COLLAT_WALLET=`lotus wallet new bls`
export PUBMSG_WALLET=`lotus wallet new bls`
export CLIENT_WALLET=`lotus wallet new bls`
```

7. Add funds to the wallets
```
lotus send --from $DEFAULT_WALLET $COLLAT_WALLET 10
lotus send --from $DEFAULT_WALLET $PUBMSG_WALLET 10
lotus send --from $DEFAULT_WALLET $CLIENT_WALLET 10
```

Run this command repeatedly until each wallet you created has 10 FIL:
```
lotus wallet list
```
This should take about 10 seconds.

8. Set the Publish Message Wallet as a control address on the miner
```
lotus-miner actor control set --really-do-it $PUBMSG_WALLET
```

9. Add funds into the Market Actor escrow for the client and Collateral wallets
```
lotus wallet market add --from $DEFAULT_WALLET --address $CLIENT_WALLET 5
lotus wallet market add --address $COLLAT_WALLET 5
```

10. Initialize boost
```
boostd -vv init \
  --api-sealer=$MINER_API_INFO \
  --api-sector-index=$MINER_API_INFO \
  --wallet-publish-storage-deals=$PUBMSG_WALLET \
  --wallet-collateral-pledge=$COLLAT_WALLET \
  --max-staging-deals-bytes=2000000000
```

11. Run boost
```
boostd -vv run
```

#### Next steps

Open http://localhost:8080 to see the Boost UI

To make a deal with boost, follow the guide at https://boost.filecoin.io/tutorials/how-to-store-files-with-boost-on-filecoin

Note that above you already ran a command to export FULLNODE_API (and point it to your local devnet lotus daemon).

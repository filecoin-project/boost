# Boost

Boost is a tool for Filecoin storage providers to manage data storage and retrievals on Filecoin.

See the docs at [https://boost.filecoin.io](https://boost.filecoin.io/getting-started) to get started.

## Building and Installing

Compile and install using the instructions at the `Building and installing` section in [the docs](https://boost.filecoin.io/getting-started#building-and-installing).

## Running Boost for development

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

Double check if environment variables are set:
```
export LIBRARY_PATH=/opt/homebrew/lib
export PATH="$(brew --prefix coreutils)/libexec/gnubin:/usr/local/bin:$PATH"
```

Build & Install
```
cd boost
make debug
make install
```

3. Start the devnet

The following command will use the lotus binaries that you built and installed above, and especially it will run `lotus`, `lotus-miner` and `lotus-seed`. So the lotus version must match the version in boost's go.mod.
```
cd boost
./devnet
```

The first time you run it, it will install a lot of metadata-related proofs. It will take at least 30 minutes depending on your connection speed. You may need to restart the command multiple times as your terminal will probably timeout before it finishes downloading everything.

You can also use `./devnet &` instead of `./devnet` to run the process in the background - that would prevent you from having to manually restart the command when your terminal times out.

NOTE: 
The devnet isn't designed to be restartable unfortunately. After it has been successfully run once, you'll have to clear out the previous data before re-running `./devnet`: 
```
rm -rf ~/.lotusmarkets && rm -rf ~/.lotus && rm -rf ~/.lotusminer && rm -rf ~/.genesis_sectors
```

4. Wait for lotus-miner to come up (through the command above)

Unset these variables as they interfere with the `lotus-miner` command.
```
unset MINER_API_INFO
unset FULLNODE_API_INFO
```
Then repeatedly run this command until it succeeds:
```
lotus-miner auth api-info --perm=admin
```

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
  --wallet-deal-collateral=$COLLAT_WALLET \
  --max-staging-deals-bytes=2000000000
```

11. Build the Web UI
```
make react
```

12. Edit config to set a fixed listen address

Open `~/.boost/config.toml`

Set the port in the `ListenAddresses` key to `50000` 
```
[Libp2p]
  ListenAddresses = ["/ip4/0.0.0.0/tcp/50000", "/ip6/::/tcp/0"]
```

13. Run boost
```
boostd -vv run
```

Note down the peer ID of the boost instance:
```
2022-06-10T09:32:28.819Z        INFO    boostd  boostd/run.go:114       Boost libp2p node listening     {"maddr": "{12D3KooWQNNWNiJ1mieEk9EHjDVF2qBc1FSjJGEzwjnMJzteApaW: [/ip4/172.17.0.2/tcp/50000 /ip4/127.0.0.1/tcp/50000]}"}
```
In this example: `12D3KooWQNNWNiJ1mieEk9EHjDVF2qBc1FSjJGEzwjnMJzteApaW`

14. Set the peer ID and multi-address of the miner on chain
```
lotus-miner actor set-peer-id <peer id>
lotus-miner actor set-addrs /ip4/127.0.0.1/tcp/50000
```

15. Open the Web UI

Open http://localhost:8080 to see the Boost UI

## Make a deal with Boost

2. Initialize the boost client
```
boost init
```

This will output the address of the wallet (it's safe to run the init command repeatedly).

2. Send funds to the client wallet
```
lotus send --from=$DEFAULT_WALLET <client wallet> 10
```

3. Follow the guide at https://boost.filecoin.io/tutorials/how-to-store-files-with-boost-on-filecoin

Note that above you already ran a command to export FULLNODE_API (and point it to your local devnet lotus daemon).

Note also that the provider address is `t01000` and you will need to supply an appropriate `--storage-price` when using `boost deal` since the devnet has a minimum price. Alternatively, using "Settings" in the Boost web UI to set the deal price to zero. 

## Running Boost devnet in Docker

## Devnet docker images for lotus and boost

This dir contains scripts for building docker images that are required to start the lotus devnet with the boost as a storage provider. It is a realization of [devnet guide](../../documentation/devnet.md) in docker containers. `lotus` and `lotus-miner` images are based on the [official lotus image file](https://github.com/filecoin-project/lotus/blob/master/Dockerfile.lotus). Because there is no image with lotus in debug mode published on Dockerhub so we rebuild lotus containers locally. 

NOTE: These docker images are for demo and devs ONLY. They MUST NOT/CAN NOT be used in production environments.

## Building Docker images:

1. Select lotus version, for example: `lotus_version=1.17.1-rc2`. It must be the tag name of [the lotus git repo](https://github.com/filecoin-project/lotus/tags) without `v` prefix.
2. Select boost version, for example: `boost_version=1.3.0-rc1`. Docker images for the boost will be built on the current code base. The `boost_version` is just used to tag images. If you want to build images for a specific boost version then you have to checkout that version first.
3. Build images

```
make build/all lotus_version=1.17.1-rc2 boost_version=1.3.0-rc1
```

## To start devnet:

1. Run
```
docker compose up -d
```
It will spin up `lotus`, `lotus-miner`, `boost`, `boost-gui` and `demo-http-server` containers. All temporary data will be saved in `./data` folder.
The initial setup could take up to 20 min or more (it takes time to download filecoin proof parameters). During the initial setup, it is normal to see error messages in the log. Containers are waiting for the lotus to be ready. It may timeout several times. Restart is expected to be managed by `docker`.

2. Try opening the boost GUI http://localhost:8000 with a browser. Devnet is ready to operate when the URL opens and indicates no errors on the startup page.
Also, you can try to inspect the status using `docker compose logs -f`.  

## Making a deal

The `boost` container is packed with `boost` and `lotus` clients. You can connect to the container with the command `docker compose exec boost /bin/bash` and follow instructions for [storing files with Boost guide](https://boost.filecoin.io/tutorials/how-to-store-files-with-boost-on-filecoin). But the recommended startup is to follow the semi-interactive demo first:
```
# attach to a running boost container
docker compose exec boost /bin/bash

# execute the demo script /app/sample/make-a-deal.sh 
root@83260455bbd2:/app# ./sample/make-a-deal.sh 
```
## Accessing lotus from localhost

By default the [docker-compose.yaml](./docker-compose.yaml) does not expose any port of the `lotus` container. To access the `lotus` from a local machine: 
1. You can either expose `1234` in [docker-compose.yaml](./docker-compose.yaml) or find the IP of the `lotus` container using `docker inspect lotus | grep IPAddress` command.
2. Get the `FULLNODE_API_INFO`
```
docker exec -it lotus lotus auth api-info --perm=admin
FULLNODE_API_INFO=eyJ...ms4:/dns/lotus/tcp/1234/http
```
3. Change the `dns/lotus/tcp/1234/http` to `ip4/<127.0.0.1 or container's IP>/tcp/1234/http` for the use in `FULLNODE_API_INFO`.

## Cleaning devnet

To stop containers and drop everything: 
```
docker compose down --rmi all

sudo rm -rf ./data
```

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/boost/blob/main/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/boost/blob/main/LICENSE-APACHE)

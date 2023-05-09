# Boost

Boost is a tool for Filecoin storage providers to manage data storage and retrievals on Filecoin.

See the docs at [https://boost.filecoin.io](https://boost.filecoin.io/getting-started) to get started.

## Table of Contents

- [Building and Installing Boost](#building-and-installing-boost)
- [Running Boost for development](#running-boost-for-development)
- [Running Boost devnet in Docker](#running-boost-devnet-in-docker)
- [License](#license)

## Building and Installing Boost

Compile and install using the instructions at the `Building and installing` section in [the docs](https://boost.filecoin.io/getting-started#building-and-installing).

## Running Boost for development

To run Boost on your development machine, you will need to set up a devnet:

1. Remove any existing Lotus and Boost repositories
```
rm -rf ~/.lotusmarkets ~/.lotus ~/.lotusminer ~/.genesis_sectors
rm -rf ~/.boost
```

2. Build Lotus in debug mode

The version of lotus needs to match the version in Boost's go.mod
```
cd lotus
git checkout <tag>
make debug
```

3. Install Lotus

The devnet script uses the installed `lotus` and `lotus-miner` binaries to run the miner and daemon.
```
make install
install -C ./lotus-seed /usr/local/bin/lotus-seed
```

4. Build Boost in debug mode

Double check if environment variables are set:
```
export LIBRARY_PATH=/opt/homebrew/lib
export PATH="$(brew --prefix coreutils)/libexec/gnubin:/usr/local/bin:$PATH"
```

Build and install
```
cd boost
make debug
make install
```

5. Start the devnet

The following command will use the binaries that you built and installed above, and will run `lotus`, `lotus-miner` and `lotus-seed`. The `lotus` version must match the version in Boost's go.mod.
```
cd boost
./devnet
```

The first time you run it, it will download the Filecoin proof parameters. It will take at least 10 minutes depending on your connection speed. You may need to restart the command multiple times as your terminal will probably timeout before it finishes downloading everything.

The devnet isn't designed to be restartable. After it has been successfully run once, you'll have to clear out the previous data before re-running `./devnet`:
```
rm -rf ~/.lotusmarkets && rm -rf ~/.lotus && rm -rf ~/.lotusminer && rm -rf ~/.genesis_sectors
```

6. Wait for `lotus-miner` to come up (through the command above)

Unset these variables as they interfere with the `lotus-miner` command.
```
unset MINER_API_INFO
unset FULLNODE_API_INFO
```

Then repeatedly run this command until it succeeds:
```
lotus-miner auth api-info --perm=admin
```

7. Get the authentication tokens to connect to the lotus daemon and miner:

```
export ENV_MINER_API_INFO=`lotus-miner auth api-info --perm=admin`
export ENV_FULLNODE_API_INFO=`lotus auth api-info --perm=admin`

export MINER_API_INFO=`echo $ENV_MINER_API_INFO | awk '{split($0,a,"="); print a[2]}'`
export FULLNODE_API_INFO=`echo $ENV_FULLNODE_API_INFO | awk '{split($0,a,"="); print a[2]}'`

echo MINER_API_INFO=$MINER_API_INFO
echo FULLNODE_API_INFO=$FULLNODE_API_INFO
```

8. Create the wallets needed for Boost

```
export DEFAULT_WALLET=`lotus wallet list | tail -1 | awk '{print $1}'`
export COLLAT_WALLET=`lotus wallet new bls`
export PUBMSG_WALLET=`lotus wallet new bls`
export CLIENT_WALLET=`lotus wallet new bls`
```

9. Add funds to the wallets

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

10. Set the Publish Message Wallet as a control address on the miner

```
lotus-miner actor control set --really-do-it $PUBMSG_WALLET
```

11. Add funds into the Market Actor escrow for the client and Collateral wallets

```
lotus wallet market add --from $DEFAULT_WALLET --address $CLIENT_WALLET 5
lotus wallet market add --address $COLLAT_WALLET 5
```

12. Initialize Boost / Create Boost repository

```
boostd -vv init \
  --api-sealer=$MINER_API_INFO \
  --api-sector-index=$MINER_API_INFO \
  --wallet-publish-storage-deals=$PUBMSG_WALLET \
  --wallet-deal-collateral=$COLLAT_WALLET \
  --max-staging-deals-bytes=2000000000
```

13. Build the Web UI
```
make react
```

14. Edit config to set a fixed listen address

Edit `~/.boost/config.toml`

Set the port in the `ListenAddresses` key to `50000`
```
[Libp2p]
  ListenAddresses = ["/ip4/0.0.0.0/tcp/50000", "/ip6/::/tcp/0"]
```

15. Run Boost
```
boostd -vv run
```

Note the peer ID of the boost instance:
```
2022-06-10T09:32:28.819Z        INFO    boostd  boostd/run.go:114       Boost libp2p node listening     {"maddr": "{12D3KooWQNNWNiJ1mieEk9EHjDVF2qBc1FSjJGEzwjnMJzteApaW: [/ip4/172.17.0.2/tcp/50000 /ip4/127.0.0.1/tcp/50000]}"}
```
In this example: `12D3KooWQNNWNiJ1mieEk9EHjDVF2qBc1FSjJGEzwjnMJzteApaW`

14. Set the peer ID and multi-address of the miner on chain
```
lotus-miner actor set-peer-id <peer id>
lotus-miner actor set-addrs /ip4/127.0.0.1/tcp/50000
```

16. Open the Web UI

Open http://localhost:8080 to see the Boost UI

### Make a deal with Boost

1. Initialize the Boost client
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
### Prerequisites
* Install Docker - https://docs.docker.com/get-docker/

### Building Docker images

1. Build images from the root of the Boost repository

```
make clean docker/all
```

On ARM-based systems (*Apple M1/M2*) you need to force building Filecoin's Rust libraries from the source
```
make clean docker/all ffi_from_source=1 build_lotus=1
```

If you need to build containers using a specific version of lotus then provide the version as a parameter, e.g. `make clean docker/all lotus_version=v1.20.0-rc2 build_lotus=1`. The version must be a tag or a remote branch name of [Lotus git repo](https://github.com/filecoin-project/lotus).

### Start devnet docker stack

1. Run

```
make devnet/up
```

It will spin up `lotus`, `lotus-miner`, `boost`, `booster-http` and `demo-http-server` containers. All temporary data will be saved in `./docker/devnet/data` folder.

The initial setup could take up to 20 min or more as it needs to download Filecoin proof parameters. During the initial setup, it is normal to see error messages in the log. Containers are waiting for the lotus to be ready. It may timeout several times. Restart is expected to be managed by `docker`.

2. Try opening the Boost GUI http://localhost:8080 . Devnet is ready to operate when the URL opens and indicates no errors on the startup page.

You can inspect the status using `cd docker/devnet && docker compose logs -f`.

### Start monitoring docker stack

```
docker plugin install grafana/loki-docker-driver:latest --alias loki --grant-all-permissions

cd docker/monitoring
docker compose up -d
```

### Connect monitoring stack to devnet stack

```
docker network connect devnet tempo
docker network connect devnet prometheus
```

### Explore Grafana / Tempo and search for traces

http://localhost:3333 (username: `admin` ; password: `admin`)

### Making a deal

The `boost` container is packed with `boost` and `lotus` clients. You can connect to the container with the command `docker compose exec boost /bin/bash` and follow instructions for [storing files with Boost guide](https://boost.filecoin.io/tutorials/how-to-store-files-with-boost-on-filecoin). But the recommended startup is to follow the semi-interactive demo first:

```
# Attach to a running boost container
make devnet/exec service=boost

# Execute the demo script /app/sample/make-a-deal.sh
root@83260455bbd2:/app# ./sample/make-a-deal.sh
```

You can also generate, dense, random cars and automatically make deals by leveraging the script at `./docker/devnet/boost/sample/random-deal.sh`. See the scripts comments for usage details.

### Accessing Lotus from localhost

By default the [docker-compose.yaml](./docker-compose.yaml) does not expose any port of the `lotus` container. To access the `lotus` from a local machine:
1. You can either expose `1234` in [docker-compose.yaml](./docker-compose.yaml) or find the IP of the `lotus` container using `docker inspect lotus | grep IPAddress` command.
2. Get the `FULLNODE_API_INFO`
```
docker exec -it lotus lotus auth api-info --perm=admin
FULLNODE_API_INFO=eyJ...ms4:/dns/lotus/tcp/1234/http

docker exec -it lotus-miner lotus-miner auth api-info --perm=admin
MINER_API_INFO=eyJ...UlI:/dns/lotus-miner/tcp/2345/http
```
3. Change the `dns/lotus/tcp/1234/http` to `ip4/<127.0.0.1 or container's IP>/tcp/1234/http` for the use in `FULLNODE_API_INFO`.

### Cleaning up

To stop containers and drop everything:
```
make devnet/down

rm -rf ~/.cache/filecoin-proof-parameters
```

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/boost/blob/main/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/boost/blob/main/LICENSE-APACHE)

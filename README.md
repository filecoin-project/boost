# Boost

Boost is a tool for Filecoin storage providers to manage data storage and retrievals on Filecoin.

It replaces the `lotus-miner` `markets` service, as well as the `go-fil-markets` package with a standalone binary that runs alongside a `lotus` daemon and `lotus-miner`.

Boost exposes `libp2p` interfaces for making storage and retrieval deals, a web interface for managing storage deals, and a GraphQL interface for accessing and updating real-time deal information.

## Building

### Prerequisites

Currently, Boost installation is supported on macOS and Linux platforms.

You need to have Go and Rust installed on your machine in order to build Boost, as well as some additional system dependencies, usually provided by your distribution: 

Ubuntu/Debian:
```
sudo apt install mesa-opencl-icd ocl-icd-opencl-dev gcc git bzr jq pkg-config curl clang build-essential hwloc libhwloc-dev wget -y && sudo apt upgrade -y
```

Arch:
```
sudo pacman -Syu opencl-icd-loader gcc git bzr jq pkg-config opencl-icd-loader opencl-headers opencl-nvidia hwloc
```

macOS:
```
brew install go bzr jq pkg-config rustup hwloc coreutils
```

Boost also requires that Xcode Command Line Tools be installed before building the Boost binaries.

### Compiling from source

```
git clone https://github.com/filecoin-project/boost
cd boost
make build
make install
```

## Table of Contents

- [Documentation](#documentation)
- [Getting started](#getting-started)
    - [For clients](#for-clients)
    - [For storage providers](#for-storage-providers)
    - [For development](#for-development)
- [Web UI](#web-ui)
- [License](#license)

## Documentation

Boost Documentation website is located at: https://boost.filecoin.io/

## Getting started

### Binaries

`boost` - Boost client for interacting with Filecoin storage providers that support Boost protocols

`boostd` - Boost daemon which is run by storage providers

`boostx` - Experimental utilities used for development and testing of Boost

### For clients

Boost comes with a client `boost` executable that can be used to send a deal proposal to a `boostd` server.

The client is intentionally minimal. It does not require a daemon process, and can be pointed at any public Filecoin gateway API for on-chain operations. This means that users of the client do not need to run a Filecoin node that syncs the chain.

1. Set the API endpoint environment variable

```
export FULLNODE_API_INFO=<filecoin API endpoint>
```

2. Initialize the client

```
boost -vv init
```

The init command:

a) Creates a Boost client repo (at ~/.boost-client by default)

b) Generates a libp2p peer ID key

c) Generates a wallet for on-chain operations and outputs the wallet address

3. Add funds to the wallet and to the market actor

To make deals you will need to:

a) add funds to the wallet;

b) add funds to the market actor for that wallet address;

4. Make a storage deal

```
boost -vv deal --provider=<f00001> \
               --http-url=<https://myserver/my.car> \
               --commp=<commp> \
               --car-size=<car-size> \
               --piece-size=<piece-size> \
               --payload-cid=<payload-cid>
```

### For storage providers

Refer to the [Documentation website](https://boost.filecoin.io).

### For development


1. Make sure you have a local Lotus fullnode and miner running and listening to `localhost:1234` and `localhost:2345` respectively, for example with a devnet:

```
devnet
```

Note that currently `devnet` is using the default paths that `lotus` and `lotus-miner` use for their repositories, and you should make sure these directories are empty:

```
LOTUS_PATH=~/.lotus
LOTUS_MINER_PATH=~/.lotusminer

rm -rf ~/.lotus ~/.lotusminer
```


2. Create Boost repository

```
export $(lotus auth api-info --perm=admin)
export $(lotus-miner auth api-info --perm=admin)

boostd --vv init \
       --api-sealer=`lotus-miner auth api-info --perm=admin` \
       --api-sector-index=`lotus-miner auth api-info --perm=admin` \
       --wallet-publish-storage-deals=`lotus wallet new bls` \
       --wallet-collateral-pledge=`lotus wallet new bls` \
       --max-staging-deals-bytes=50000000000
```

3. Run the Boost daemon service

```
export $(lotus auth api-info --perm=admin)

boostd --vv run
```

4. Interact with Boost

Pass the client address (wallet) and the provider address to the `dummydeal` command.
Note that
- the client address is the address of a wallet with funds in `lotus wallet list`
- you can find the provider address in `~/.boost/config.toml` under the config key `Wallets.Miner`

```
boostd dummydeal <client address> <provider address>
```

## Web UI

1. Install and build

```
cd react
npm install
npm run build
```

2. Open UI

```
http://localhost:8080
```

### Development mode

To run the web UI in development mode:

1. Run the server

```
cd react
npm install
npm start
```

2. Open UI

```
http://localhost:3000
```

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/boost/blob/main/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/boost/blob/main/LICENSE-APACHE)

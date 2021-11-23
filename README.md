# Boost

Boost is an initial experiment/prototype of markets v2 module for Filecoin. It aims to replace the `lotus-miner` markets service. For more information, see: https://docs.filecoin.io/mine/lotus/split-markets-miners/

## Building

```
git clone https://github.com/filecoin-project/boost
cd boost
make build
```

## Initialisation and Running

0. Compile and install (move binaries to $PATH)

```
make build
make install
```

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
export FULLNODE_API_INFO=/ip4/127.0.0.1/tcp/1234/http
boost --vv init \
      --api-sector-index=`lotus-miner auth api-info --perm=admin` \
      --wallet-publish-storage-deals=`lotus wallet new bls` \
      --wallet-collateral-pledge=`lotus wallet new bls`
```

3. Run Boost service

```
boost run
```

4. Interact with Boost

Pass the client address (wallet) and the provider address to the `dummydeal` command.
Note that
- the client address is the address of a wallet with funds in `lotus wallet list`
- you can find the provider address in `~/.boost/config.toml` under the config key `Wallets.Miner`

```
export FULLNODE_API_INFO=`lotus auth api-info --perm=admin`
boost dummydeal <client address> <provider address>
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

# Boost

Boost is a tool for Filecoin storage providers to manage data storage and retrievals on Filecoin.

See [Documentation](https://boost.filecoin.io/) to get started.

## For development:


1. Install using instructions in [Documentation](https://boost.filecoin.io/).

2. Make sure you have a local Lotus fullnode and miner running and listening to `localhost:1234` and `localhost:2345` respectively, for example with a devnet:

```
devnet
```

Note that currently `devnet` is using the default paths that `lotus` and `lotus-miner` use for their repositories, and you should make sure these directories are empty:

```
LOTUS_PATH=~/.lotus
LOTUS_MINER_PATH=~/.lotusminer

rm -rf ~/.lotus ~/.lotusminer
```


3. Create Boost repository

```
export $(lotus auth api-info --perm=admin)
export $(lotus-miner auth api-info --perm=admin)

boostd --vv init \
       --api-sealer=`lotus-miner auth api-info --perm=admin` \
       --api-sector-index=`lotus-miner auth api-info --perm=admin` \
       --wallet-publish-storage-deals=`lotus wallet new bls` \
       --wallet-deal-collateral=`lotus wallet new bls` \
       --max-staging-deals-bytes=50000000000
```

4. Run the Boost daemon service

```
export $(lotus auth api-info --perm=admin)

boostd --vv run
```

5. Interact with Boost

Pass the client address (wallet) and the provider address to the `dummydeal` command.
Note that
- the client address is the address of a wallet with funds in `lotus wallet list`
- you can find the provider address in `~/.boost/config.toml` under the config key `Wallets.Miner`

```
boostd dummydeal <client address> <provider address>
```

## Running the UI in Development Mode:

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

## Running a devnet:

Follow the instructions in the [devnet guide](./documentation/devnet.md)

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/boost/blob/main/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/boost/blob/main/LICENSE-APACHE)

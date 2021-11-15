# Boost

Boost is an initial experiment/prototype of markets v2 module for Filecoin. It aims to replace the `lotus-miner` markets service. For more information, see: https://docs.filecoin.io/mine/lotus/split-markets-miners/

## Building

```
git clone https://github.com/filecoin-project/boost
cd boost
make boost
```

## Initialisation and Running

0. Make sure you have a local Lotus fullnode running and listening to `localhost:1234`, for example with a devnet:

```
# 1. init & start lotus node

rm -rf ~/.devlotus && export LOTUS_BACKUP_BASE_PATH=~/.lotusbackup && export LOTUS_PATH=~/.devlotus && rm -rf localnet.json &&  ./lotus-seed genesis new localnet.json  && ./lotus-seed pre-seal --sector-size 2048 --num-sectors 10 && ./lotus-seed genesis add-miner localnet.json ~/.genesis-sectors/pre-seal-t01000.json && clear && ./lotus daemon --lotus-make-genesis=dev.gen --genesis-template=localnet.json --bootstrap=false

# 2. init & start lotus-miner

rm -rf ~/.lotusminer && export LOTUS_BACKUP_BASE_PATH=~/.lotusbackup && export LOTUS_PATH=~/.devlotus && ./lotus wallet import ~/.genesis-sectors/pre-seal-t01000.key && ./lotus-miner init --genesis-miner --actor=t01000 --sector-size=2048 --pre-sealed-sectors=~/.genesis-sectors --pre-sealed-metadata=~/.genesis-sectors/pre-seal-t01000.json --nosync && ./lotus-miner run --nosync
```

1. Create Boost repository

```
FULLNODE_API_INFO=/ip4/127.0.0.1/tcp/1234/http boost init
```

2. Run Boost service

```
FULLNODE_API_INFO=/ip4/127.0.0.1/tcp/1234/http boost run
```

3. Interact with Boost

```
boost dummydeal
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

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/boost/blob/main/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/boost/blob/main/LICENSE-APACHE)

# Boost

Boost is an initial experiment/prototype of markets v2 module for Filecoin. It aims to replace the `lotus-miner` markets service. For more information, see: https://docs.filecoin.io/mine/lotus/split-markets-miners/

## Building

```
git clone https://github.com/filecoin-project/boost
cd boost
make boost
```

## Initialisation and Running

0. Make sure you have a local Lotus fullnode running and listening to `localhost:1234`, for example:

```
ssh -L 1234:localhost:1234 sofiaminer
```

1. Create Boost repository

```
boost init
```

2. Run Boost service

```
boost run
```

3. Interact with Boost

```
boost dummydeal
```

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/boost/blob/main/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/boost/blob/main/LICENSE-APACHE)

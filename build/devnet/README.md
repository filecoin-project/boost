# Devnet docker images for lotus and boost

This dir contains scripts for building docker images that are required to start the lotus devnet with the boost as a storage provider. It is realization of [devnet guide](../../documentation/devnet.md) in docker containers. `lotus` and `lotus-miner` images are based on the [official lotus image file](https://github.com/filecoin-project/lotus/blob/master/Dockerfile.lotus) as there is no image with lotus in debug mode published on Dockerhub. 

NOTE: These docker images are for demo and devs ONLY. They MUST NOT/CAN NOT be used in production environments.

## Building images:

1. Select lotus version, for example: `lotus_version=1.17.1-rc2`. It must be the tag name of [the lotus git repo](https://github.com/filecoin-project/lotus/tags) without `v` prefix.
2. Select boost version, for example: `boost_version=1.3.0-rc1`. Docker images for the boost will be built on the current code base. The `boost_version` is just used to tag images. It you want to build images for a specific boost version then you have to chechout that version first.
3. Build images:

```
make build/all lotus_version=1.17.1-rc2 boost_version=1.3.0-rc1
```
## Publishing images:

1. Login to docker with `filecoin` user.
2. Publish
```
make push/all lotus_version=1.17.1-rc2 boost_version=1.3.0-rc1
```
3. If you want to publish using a non `filecoin` account (for some testing purposes)

```
make push/all lotus_version=1.17.1-rc2 boost_version=1.3.0-rc1 docker_user=<some_user>
```
## How to run devnet in docker:

Follow the instructions in the [docker-compose devnet guide](../../examples/devnet/README.md)
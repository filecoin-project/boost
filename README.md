# Boost

Boost is a tool for Filecoin storage providers to manage data storage and retrievals on Filecoin.

See the docs at [https://boost.filecoin.io](https://boost.filecoin.io/getting-started) to get started.

## Table of Contents

- [Building and Installing Boost](#building-and-installing-boost)
- [Running Boost devnet in Docker](#running-boost-devnet-in-docker-for-development)
- [External Contribution Guidelines](#external-contribution-guidelines)
- [License](#license)

## Building and Installing Boost

Compile and install using the instructions at the `Building and installing` section in [the docs](https://boost.filecoin.io/getting-started#building-and-installing).

## Running Boost devnet in Docker for development

### Pre-requisites
* Install Docker - https://docs.docker.com/get-docker/

### Building Docker images

1. Build images from the root of the Boost repository

```
make clean docker/all
```

If you need to build containers using a specific version of lotus then provide the version as a parameter, e.g. `make clean docker/all lotus_version=v1.23.3`. The version must be a tag or a remote branch name of [Lotus git repo](https://github.com/filecoin-project/lotus).
If the branch or tag you requested does not exist in our [Github image repository](https://github.com/filecoin-shipyard/lotus-containers/pkgs/container/lotus-containers) then you can build the lotus image manually with  `make clean docker/all lotus_version=test/branch1 build_lotus=1`. We are shipping images all releases from Lotus in our [Github image repo](https://github.com/filecoin-shipyard/lotus-containers/pkgs/container/lotus-containers).

### Start devnet Docker stack

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

## External Contribution Guidelines
If you want to contribute to the Boost project, please refer to [these guidelines](./CONTRIBUTING.md). 

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/boost/blob/main/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/boost/blob/main/LICENSE-APACHE)

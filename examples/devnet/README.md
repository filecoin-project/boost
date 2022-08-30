# Devnet in docker

The docker-compose file contains a realization of the [devnet guide](../../documentation/devnet.md) in docker containers. 

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

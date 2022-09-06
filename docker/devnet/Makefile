##################################################################################
lotus_version?=1.17.1-rc2
boost_version?=1.3.0-rc1
docker_user?=filecoin

lotus_test_image=$(docker_user)/lotus-test:$(lotus_version)
##################################################################################
lotus-$(lotus_version):
	git clone --depth 1 --branch v$(lotus_version) https://github.com/filecoin-project/lotus $@

prepare/lotus-test: | lotus-$(lotus_version)
	cd lotus-$(lotus_version) && docker build -f Dockerfile.lotus --target lotus-test -t $(lotus_test_image) .
.PHONY: prepare/lotus-test
##################################################################################
build/%: prepare/lotus-test
	cd $* && make dbuild
../../build/.update-modules: 
	cd ../../ && make build/.update-modules	
build/boost: ../../build/.update-modules prepare/lotus-test
	cd boost && make dbuild
push/%: prepare/lotus-test
	cd $* && make dpush	
##################################################################################
build/all: build/lotus build/lotus-miner build/boost build/boost-gui
.PHONY: build/all
##################################################################################
push/all: push/lotus push/lotus-miner push/boost push/boost-gui
.PHONY: push/all
##################################################################################
clean: clean/lotus-test
.PHONY: clean

clean/lotus-test:
	rm -rf lotus-$(lotus_version)
.PHONY: clean/lotus-test

.EXPORT_ALL_VARIABLES:
##################################################################################
start: 
	docker compose up -d
	docker compose logs -f
.PHONY: start
##################################################################################
ssh/boost: 
	docker compose exec boost /bin/bash
.PHONY: ssh/boost
##################################################################################
clean-stack: clean/docker
	rm -rf data
.PHONY: clean
clean/all: clean
	rm -rf /var/tmp/filecoin-proof-parameters
.PHONY: clean/all
clean/docker:
	docker compose down
.PHONY: clean/docker
##################################################################################

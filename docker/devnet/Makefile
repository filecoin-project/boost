##################################################################################
# building lotus
#  - from source, provide "lotus_src_dir", e.g.: 
#      make build/all lotus_src_dir=../../../lotus 
#  - specific version (default) -> provide "lotus_version", e.g:
#      make build/all lotus_versionr=1.17.0 
##################################################################################
lotus_version?=1.17.1-rc2
lotus_src_dir=
boost_version?=1.3.0-rc1
docker_user?=filecoin

ifeq ($(lotus_src_dir),)
    lotus_src_dir=/tmp/lotus-$(lotus_version)
    lotus_checkout_dir=$(lotus_src_dir)
else
    lotus_version=dev
    lotus_checkout_dir=
endif
lotus_test_image=$(docker_user)/lotus-test:$(lotus_version)
##################################################################################
info/lotus-test:
	@echo Lotus dir = $(lotus_src_dir)
	@echo Lotus ver = $(lotus_version)
.PHONY: info/lotus-test	
$(lotus_checkout_dir):
	git clone --depth 1 --branch v$(lotus_version) https://github.com/filecoin-project/lotus $@
prepare/lotus-test: info/lotus-test | $(lotus_checkout_dir)
	cd $(lotus_src_dir) && docker build -f Dockerfile.lotus --target lotus-test -t $(lotus_test_image) .
.PHONY: prepare/lotus-test
##################################################################################
build/%: prepare/lotus-test
	cd $* && make dbuild
build/boost:
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

# clean/lotus-test - removes temporary lotus dir at /tmp/lotus-$(lotus_version)
clean/lotus-test:
	rm -rf $(lotus_checkout_dir)
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

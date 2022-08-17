lotus_version?=v1.17.1-rc2
boost_version?=1.3.0-rc1
docker_user?=filecoin

lotus_test_image=$(docker_user)/lotus-test:$(lotus_version)
##################################################################################
lotus-$(lotus_version):
	git clone --depth 1 --branch $(lotus_version) https://github.com/filecoin-project/lotus $@

prepare/lotus-test: | lotus-$(lotus_version)
	cd lotus-$(lotus_version) && docker build -f Dockerfile.lotus --target lotus-test -t $(lotus_test_image) .
.PHONY: prepare/lotus-test
##################################################################################
##################################################################################
build/%: prepare/lotus-test
	cd $* && make dbuild
##################################################################################
build/all: build/lotus build/lotus-miner build/boost build/boost-gui
.PHONY: build/all

##################################################################################
clean: clean/lotus-test
.PHONY: clean

clean/lotus-test:
	rm -rf lotus-$(lotus_version)
.PHONY: clean/lotus-test

.EXPORT_ALL_VARIABLES:
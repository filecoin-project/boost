SHELL=/usr/bin/env bash

all: build
.PHONY: all

unexport GOFLAGS

GOCC?=go

GOVERSION:=$(shell $(GOCC) version | tr ' ' '\n' | grep go1 | sed 's/^go//' | awk -F. '{printf "%d%03d%03d", $$1, $$2, $$3}')
ifeq ($(shell expr $(GOVERSION) \< 1016000), 1)
$(warning Your Golang version is go$(shell expr $(GOVERSION) / 1000000).$(shell expr $(GOVERSION) % 1000000 / 1000).$(shell expr $(GOVERSION) % 1000))
$(error Update Golang to version to at least 1.16.0)
endif

LTS_NODE_VER=16
NODE_VER=$(shell node -v)
ifeq ($(patsubst v$(LTS_NODE_VER).%,matched,$(NODE_VER)), matched)
	NODE_LTS=true
else
	NODE_LTS=false
endif

# git modules that need to be loaded
MODULES:=

CLEAN:=
BINS:=

ldflags=-X=github.com/filecoin-project/boost/build.CurrentCommit=+git.$(subst -,.,$(shell git describe --always --match=NeVeRmAtCh --dirty 2>/dev/null || git rev-parse --short HEAD 2>/dev/null))
ifneq ($(strip $(LDFLAGS)),)
	ldflags+=-extldflags=$(LDFLAGS)
endif

GOFLAGS+=-ldflags="$(ldflags)"


## FFI

FFI_PATH:=extern/filecoin-ffi/
FFI_DEPS:=.install-filcrypto
FFI_DEPS:=$(addprefix $(FFI_PATH),$(FFI_DEPS))

$(FFI_DEPS): build/.filecoin-install ;

build/.filecoin-install: $(FFI_PATH)
	$(MAKE) -C $(FFI_PATH) $(FFI_DEPS:$(FFI_PATH)%=%)
	@touch $@

MODULES+=$(FFI_PATH)
BUILD_DEPS+=build/.filecoin-install
CLEAN+=build/.filecoin-install

ffi-version-check:
	@[[ "$$(awk '/const Version/{print $$5}' extern/filecoin-ffi/version.go)" -eq 3 ]] || (echo "FFI version mismatch, update submodules"; exit 1)
BUILD_DEPS+=ffi-version-check

.PHONY: ffi-version-check

$(MODULES): build/.update-modules ;
# dummy file that marks the last time modules were updated
build/.update-modules:
	git submodule update --init --recursive
	touch $@

# end git modules

## MAIN BINARIES

CLEAN+=build/.update-modules

debug: GOFLAGS+=-tags=debug
debug: build-go

calibnet-go: GOFLAGS+=-tags=calibnet
calibnet-go: build-go

hyperspacenet-go: GOFLAGS+=-tags=hyperspacenet
hyperspacenet-go: build-go

deps: $(BUILD_DEPS)
.PHONY: deps

boostx: $(BUILD_DEPS)
	rm -f boostx
	$(GOCC) build $(GOFLAGS) -o boostx ./cmd/boostx
.PHONY: boostx
BINS+=boostx

boost: $(BUILD_DEPS)
	rm -f boost boostd boostx
	$(GOCC) build $(GOFLAGS) -o boost ./cmd/boost
	$(GOCC) build $(GOFLAGS) -o boostx ./cmd/boostx
	$(GOCC) build $(GOFLAGS) -o boostd ./cmd/boostd
.PHONY: boost
BINS+=boost boostx boostd

booster-http: $(BUILD_DEPS)
	rm -f booster-http
	$(GOCC) build $(GOFLAGS) -o booster-http ./cmd/booster-http
.PHONY: booster-http
BINS+=booster-http

booster-bitswap: $(BUILD_DEPS)
	rm -f booster-bitswap
	$(GOCC) build $(GOFLAGS) -o booster-bitswap ./cmd/booster-bitswap
.PHONY: booster-bitswap
BINS+=booster-bitswap

devnet: $(BUILD_DEPS)
	rm -f devnet
	$(GOCC) build $(GOFLAGS) -o devnet ./cmd/devnet
.PHONY: devnet
BINS+=devnet

boostci: $(BUILD_DEPS)
	rm -f boostci
	$(GOCC) build $(GOFLAGS) -o boostci ./cmd/boostci
.PHONY: boostci

react: check-node-lts
	npm_config_legacy_peer_deps=yes npm ci --no-audit --prefix react
	npm run --prefix react build
.PHONY: react

update-react: check-node-lts
	npm_config_legacy_peer_deps=yes npm install --no-audit --prefix react
	npm run --prefix react build
.PHONY: react

.PHONY: check-node-lts
check-node-lts:
	@$(NODE_LTS) || echo Build requires Node v$(LTS_NODE_VER) \(detected Node $(NODE_VER)\)
	@$(NODE_LTS) && echo Building using Node v$(LTS_NODE_VER)

build-go: boost devnet
.PHONY: build-go

build: react build-go
.PHONY: build

calibnet: react calibnet-go
.PHONY: calibnet

hyperspacenet: react hyperspacenet-go
.PHONY: hyperspacenet

install: install-boost install-devnet

install-boost:
	install -C ./boost /usr/local/bin/boost
	install -C ./boostd /usr/local/bin/boostd
	install -C ./boostx /usr/local/bin/boostx

install-devnet:
	install -C ./devnet /usr/local/bin/devnet

buildall: $(BINS)

clean:
	rm -rf $(CLEAN) $(BINS)
	-$(MAKE) -C $(FFI_PATH) clean
.PHONY: clean

dist-clean:
	git clean -xdff
	git submodule deinit --all -f
.PHONY: dist-clean

gen: cfgdoc-gen api-gen
.PHONY: gen

api-gen:
	$(GOCC) run ./gen/api
	goimports -w api
	goimports -w api
.PHONY: api-gen

cfgdoc-gen:
	$(GOCC) run ./node/config/cfgdocgen > ./node/config/doc_gen.go

print-%:
	@echo $*=$($*)

cbor-gen:
	pushd ./storagemarket/types && rm -rf types_cbor_gen.go && $(GOCC) generate types.go && popd

docsgen: docsgen-md docsgen-openrpc

docsgen-md-bin: api-gen
	$(GOCC) build $(GOFLAGS) -o docgen-md ./api/docgen/cmd
docsgen-openrpc-bin: api-gen
	$(GOCC) build $(GOFLAGS) -o docgen-openrpc ./api/docgen-openrpc/cmd

docsgen-md: docsgen-md-boost

docsgen-md-boost: docsgen-md-bin
	./docgen-md "api/api.go" "Boost" "api" "./api" > documentation/en/api-v1-methods.md

docsgen-openrpc: docsgen-openrpc-boost

docsgen-openrpc-boost: docsgen-openrpc-bin
	./docgen-openrpc "api/api.go" "Boost" "api" "./api" -gzip > build/openrpc/boost.json.gz

.PHONY: docsgen docsgen-md-bin docsgen-openrpc-bin

## DOCKER IMAGES
docker_user?=filecoin
lotus_version?=v1.20.0-rc1
lotus_src_dir?=
ffi_from_source?=0
build_lotus?=0
ifeq ($(lotus_src_dir),)
	ifeq ($(build_lotus),1)
# v1: building lotus image with provided lotus version
		lotus_info_msg=!!! building lotus base image from github: tag $(lotus_version) !!!
		lotus_src_dir=/tmp/lotus-$(lotus_version)
		lotus_checkout_dir=$(lotus_src_dir)
		lotus_build_cmd=docker/lotus-all-in-one
		lotus_base_image=$(docker_user)/lotus-all-in-one:dev
	else
# v2 (defaut): using lotus image
		lotus_base_image?=filecoin/lotus-all-in-one:$(lotus_version)-debug
		lotus_info_msg=using lotus image from dockerhub: $(lotus_base_image)
		lotus_build_cmd=info/lotus-all-in-one
	endif
else
# v3: building lotus image from source
	lotus_info_msg=!!! building lotus base image from source: $(lotus_src_dir) !!!
	lotus_base_image=$(docker_user)/lotus-all-in-one:dev
	lotus_build_cmd=docker/lotus-all-in-one
    lotus_checkout_dir=
endif
docker_build_cmd=docker build --build-arg LOTUS_TEST_IMAGE=$(lotus_base_image) \
	--build-arg FFI_BUILD_FROM_SOURCE=$(ffi_from_source) $(docker_args)
### lotus-all-in-one docker image build
info/lotus-all-in-one:
	@echo Docker build info: $(lotus_info_msg)
.PHONY: info/lotus-all-in-one
$(lotus_checkout_dir):
	git clone --depth 1 --branch $(lotus_version) https://github.com/filecoin-project/lotus $@
docker/lotus-all-in-one: info/lotus-all-in-one | $(lotus_checkout_dir)
# new lotus Dockerfile does not exist for older lotus versions
# temporary use the old Dockerfile.lotus
	cd $(lotus_src_dir) && $(docker_build_cmd) -f Dockerfile.lotus --target lotus-test \
		-t $(lotus_base_image) .
# code using new lotus Dockerfile
#	cd $(lotus_src_dir) && $(docker_build_cmd) -f Dockerfile --target lotus-all-in-one \
#		-t $(lotus_base_image) --build-arg GOFLAGS=-tags=debug .
.PHONY: docker/lotus-all-in-one

### devnet images
docker/%:
	cd docker/devnet/$* && DOCKER_BUILDKIT=1 $(docker_build_cmd) -t $(docker_user)/$*-dev:dev \
		--build-arg BUILD_VERSION=dev .
docker/boost: build/.update-modules
	DOCKER_BUILDKIT=1 $(docker_build_cmd) \
		-t $(docker_user)/boost-dev:dev --build-arg BUILD_VERSION=dev \
		-f docker/devnet/Dockerfile.source --target boost-dev .
.PHONY: docker/boost
docker/booster-http:
	DOCKER_BUILDKIT=1 $(docker_build_cmd) \
		-t $(docker_user)/booster-http-dev:dev --build-arg BUILD_VERSION=dev \
		-f docker/devnet/Dockerfile.source --target booster-http-dev .
.PHONY: docker/booster-http
docker/booster-bitswap:
	DOCKER_BUILDKIT=1 $(docker_build_cmd) \
		-t $(docker_user)/booster-bitswap-dev:dev --build-arg BUILD_VERSION=dev \
		-f docker/devnet/Dockerfile.source --target booster-bitswap-dev .
.PHONY: docker/booster-bitswap
docker/all: $(lotus_build_cmd) docker/boost docker/booster-http docker/booster-bitswap \
	docker/lotus docker/lotus-miner
.PHONY: docker/all

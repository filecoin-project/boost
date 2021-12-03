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
debug: build

deps: $(BUILD_DEPS)
.PHONY: deps

boost: $(BUILD_DEPS)
	rm -f boost
	$(GOCC) build $(GOFLAGS) -o boost ./cmd/boost
.PHONY: boost
BINS+=boost

devnet: $(BUILD_DEPS)
	rm -f devnet
	$(GOCC) build $(GOFLAGS) -o devnet ./cmd/devnet
.PHONY: devnet
BINS+=devnet

build: boost devnet
.PHONY: build

install: install-boost install-devnet

install-boost:
	install -C ./boost /usr/local/bin/boost

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

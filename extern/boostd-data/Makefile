SHELL=/usr/bin/env bash

unexport GOFLAGS

GOCC?=go

ldflags=-X=github.com/filecoin-project/boost/extern/boostd-data/build.CurrentCommit=+git.$(subst -,.,$(shell git describe --always --match=NeVeRmAtCh --dirty 2>/dev/null || git rev-parse --short HEAD 2>/dev/null))
ifneq ($(strip $(LDFLAGS)),)
	ldflags+=-extldflags=$(LDFLAGS)
endif

GOFLAGS+=-ldflags="$(ldflags)"

build:
	rm -f boostd-data
	$(GOCC) build $(GOFLAGS) -o boostd-data ./cmd
.PHONY: build

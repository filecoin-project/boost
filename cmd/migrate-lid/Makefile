SHELL=/usr/bin/env bash

unexport GOFLAGS

GOCC?=go

ldflags=-X=github.com/filecoin-project/boost/build.CurrentCommit=+git.$(subst -,.,$(shell git describe --always --match=NeVeRmAtCh --dirty 2>/dev/null || git rev-parse --short HEAD 2>/dev/null))
ifneq ($(strip $(LDFLAGS)),)
	ldflags+=-extldflags=$(LDFLAGS)
endif

GOFLAGS+=-ldflags="$(ldflags)"

build:
	rm -f migrate-lid
	$(GOCC) build $(GOFLAGS) -o migrate-lid .
.PHONY: build

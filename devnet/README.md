# devnet

Runs a Lotus daemon and miner to be used for boost integration tests.

To run it locally, install ./cmd/devnet and the lotus binaries into your $PATH, and
simply run one of the integration tests - it should start devnet automatically.

### Building and installing Lotus

	git clone https://github.com/filecoin-project/lotus
	cd lotus
	git checkout master # or whichever version
	make debug
	sudo make install # or "cp lotus lotus-*" into your $PATH

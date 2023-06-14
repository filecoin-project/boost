# devnet

Runs a Lotus daemon and miner to be used for boost integration tests.

To run it locally, install ./cmd/devnet and the lotus binaries into your $PATH, and
simply run one of the integration tests - it should start devnet automatically.

### Building and installing Lotus-related binaries

    git clone https://github.com/filecoin-project/lotus
    cd lotus
    git checkout master # or whichever version
    make debug
    sudo make install
    install -C ./lotus-seed /usr/local/bin/lotus-seed

### Clean up state directories

Make sure your $LOTUS_PATH and $LOTUS_MINER_PATH are clean before running devnet:

    rm -rf ~/.lotus
    rm -rf ~/.lotusminer

### Running using existing state directories

To use existing state directories and config the devnet can be run using `./devnet -i=false`. This skips trying to initialize the devnet and will use the existing directories.

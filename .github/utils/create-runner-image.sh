#!/bin/bash

# Directory where the GitHub Action runner will be set up
RUNNER_DIR="/tmp/github-runner"

# Clone the necessary repositories and set up the runner
setup_github_runner() {
    # Remove any existing directory
    rm -rf $RUNNER_DIR

    # Create the directory
    mkdir -p $RUNNER_DIR

    # Navigate to the directory
    # shellcheck disable=SC2164
    cd $RUNNER_DIR

    # Clone the required repositories
    git clone https://github.com/filecoin-project/curio.git
    git clone https://github.com/filecoin-project/boost.git

    # Copy necessary files
    cp -r boost/.github/utils/* .

    # Copy the Dockerfile
    cp boost/.github/image/Dockerfile .

    # Build the Docker image
    docker buildx build -t curio/github-runner:latest .
}

# Execute the function
docker system prune -a -f
setup_github_runner

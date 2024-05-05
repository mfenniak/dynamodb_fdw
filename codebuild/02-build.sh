#!/bin/bash

set -eux -o pipefail

###########################################################
# Build nix-based app & docker container

nix build --print-build-logs ".#docker"

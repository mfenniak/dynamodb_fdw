#!/bin/bash

set -eux -o pipefail

###########################################################
# Prepare version numbers for the build

sed -i"" -e "s/9.9/$VERSION/" ./flake.nix

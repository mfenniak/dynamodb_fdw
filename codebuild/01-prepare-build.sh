#!/bin/bash

set -eux -o pipefail

###########################################################
# Prepare version numbers for the build

sed -i"" -e "s/000000000000000000/$VERSION/" ./flake.nix

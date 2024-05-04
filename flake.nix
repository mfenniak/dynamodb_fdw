{
  description = "yycpathways nix development environment";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    mfenniak = {
      url = "github:mfenniak/custom-nixpkgs?dir=flake";
      # url = "/home/mfenniak/Dev/custom-nixpkgs/flake";
      inputs.flake-utils.follows = "flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, mfenniak }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};
      postgresql = pkgs.postgresql;
      python = pkgs.python3;
    in {
      devShells.default =
        pkgs.mkShell {
          buildInputs = [];
          packages = [
            (python.withPackages (python-pkgs: [
              # select Python packages here
              python-pkgs.boto3
              python-pkgs.simplejson
              (mfenniak.packages.${system}.multicorn2Python postgresql python)
            ]))
            (pkgs.postgresql.withPackages (p: [
              (mfenniak.packages.${system}.multicorn2 postgresql python)
            ]))
          ];
        };
    });
}

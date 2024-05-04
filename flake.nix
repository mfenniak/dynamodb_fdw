{
  description = "dynamodb_fdw nix development environment";

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
      devShells.default = pkgs.mkShell {
        buildInputs = [
          (python.withPackages (python-pkgs: [
            python-pkgs.boto3
            python-pkgs.simplejson
            (mfenniak.packages.${system}.multicorn2Python postgresql python)
          ]))
          (postgresql.withPackages (p: [
            (mfenniak.packages.${system}.multicorn2 postgresql python)
          ]))
        ];
        packages = [
        ];
        shellHook = ''
          export PYTHONPATH=''${PYTHONPATH:+$PYTHONPATH:}$PWD
        '';
      };

      packages = let
        fdwVersion = "9.9";
        fdwPackage = python: python.pkgs.buildPythonPackage rec {
          pname = "dynamodb_fdw";
          version = fdwVersion;

          src = [
            ./setup.py
            ./dynamodb_fdw
          ];
          unpackPhase = ''
            for srcFile in $src; do
              echo "cp $srcFile ..."
              cp -r $srcFile $(stripHash $srcFile)
            done
          '';
        };
        pythonWithDynamodb_fdw = python.withPackages (python-pkgs: [
          python-pkgs.boto3
          python-pkgs.simplejson
          (mfenniak.packages.${system}.multicorn2Python postgresql python)
          fdwPackage
        ]);
        postgresqlWithDynamodb_fdw = postgresql.withPackages (p: [
          (mfenniak.packages.${system}.multicorn2 postgresql python)
        ]);

        # Write an init script for the docker container that will check /data for a postgresql.conf file; if not
        # present, it will run initdb; and then it will startup PostgreSQL.
        initScript = ''
          #!${pkgs.runtimeShell}
          set -e
          if [ ! -f /data/postgresql.conf ]; then
            ${postgresqlWithDynamodb_fdw}/bin/initdb -D /data
            ${pkgs.gnused}/bin/sed -i "s/#unix_socket_directories = '\/run\/postgresql'/unix_socket_directories = '''/" /data/postgresql.conf
            ${pkgs.gnused}/bin/sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /data/postgresql.conf
            echo ""
            echo "*** Warning: PostgreSQL has been initialized to allow access without a password. ***"
            echo "*** This is insecure and should only be used for development purposes. ***"
            echo ""
            echo "host all all all trust" >> /data/pg_hba.conf
          fi
          ${postgresqlWithDynamodb_fdw}/bin/postgres -D /data
        '';

        # Package the initScript into a simple derivation with an executable version of the script.

        initScriptPackage = pkgs.writeScriptBin "initScript" initScript;
      in {
        # "Test":
        #   nix build .#pythonWithDynamodb_fdw && ./result/bin/python -c "from dynamodbfdw import dynamodbfdw; dynamodbfdw.DynamoFdw"
        pythonWithDynamodb_fdw = pythonWithDynamodb_fdw;

        # "Test":
        #   nix build .#docker && podman load -i result -q && podman run --rm -it -p 5432:5432 -v $HOME/.aws:/home/postgres/.aws ghcr.io/mfenniak/dynamodb_fdw:9.9
        docker = pkgs.dockerTools.buildLayeredImage {
          name = "ghcr.io/mfenniak/dynamodb_fdw";
          tag = fdwVersion;
          maxLayers = 5;

          contents = [
            pkgs.bash
            pkgs.coreutils
            pythonWithDynamodb_fdw
            postgresqlWithDynamodb_fdw
          ];

          extraCommands = ''
            #!${pkgs.runtimeShell}
            mkdir -p data
          '';

          fakeRootCommands = ''
            #!${pkgs.runtimeShell}
            set -e
            ${pkgs.dockerTools.shadowSetup}
            groupadd --system -g 999 postgres
            useradd --system --no-create-home -u 999 -g 999 postgres
            chown -R postgres:postgres data
          '';
          enableFakechroot = true;

          config = {
            User = "postgres";
            Cmd = [
              "${initScriptPackage}/bin/initScript"
            ];
          };
        };
      };
    });
}

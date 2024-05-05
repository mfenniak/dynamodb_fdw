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
      postgresql = pkgs.postgresql;  # .overrideAttrs (oldAttrs: { dontStrip = true; });  If debug symbols are needed.
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

      devShells.codebuild =
        pkgs.mkShell {
          buildInputs = [ ];
          packages = [
            pkgs.awscli2
            pkgs.curl
            pkgs.docker
            pkgs.gnused
            pkgs.jq
          ];
        };

      packages = let
        fdwVersion = "9.9";
        fdwPackage = python: python.pkgs.buildPythonPackage rec {
          pname = "dynamodb_fdw";
          version = fdwVersion;

          src = [
            ./setup.py
            ./dynamodbfdw
          ];
          unpackPhase = ''
            for srcFile in $src; do
              echo "cp $srcFile ..."
              cp -r $srcFile $(stripHash $srcFile)
            done
          '';
          doCheck = false;
        };
        pythonWithDynamodb_fdw = python.withPackages (python-pkgs: [
          python-pkgs.boto3
          python-pkgs.simplejson
          (mfenniak.packages.${system}.multicorn2Python postgresql python)
          (fdwPackage python)
        ]);
        postgresqlWithDynamodb_fdw = postgresql.withPackages (p: [
          (
            (mfenniak.packages.${system}.multicorn2 postgresql python)
            # .overrideAttrs (oldAttrs: { dontStrip = true; })   If debug symbols are needed.
          )
        ]);

        # Write an init script for the docker container that will check /data for a postgresql.conf file; if not
        # present, it will run initdb; and then it will startup PostgreSQL.
        postgresInitScript = ''
          #!${pkgs.runtimeShell}
          set -eu -o pipefail
          if [ ! -f /data/postgresql.conf ]; then
            # Note: the default encoding of SQL_ASCII is not an encoding that Python recognizes; this can cause
            # PyString_AsString in Multicorn to fail as it attempts to take the encoding of the database and convert it to
            # a string; SQL_ASCII is not a valid encoding for this purpose; all error handling in Multicorn uses this
            # function; so all errors from Multicorn will fail with a segfault as they infinitely recurse through the
            # error handling code.  Workaround: set the encoding to UTF8.
            # Multicorn seems to try to avoid this... https://github.com/pgsql-io/multicorn2/blob/19d9ef571baa21833d75e4d587807bca19de5efe/src/python.c#L104-L114
            # but this function isn't used in PyString_AsString... https://github.com/pgsql-io/multicorn2/blob/19d9ef571baa21833d75e4d587807bca19de5efe/src/python.c#L172
            # Should probably be reported upstream...
            ${postgresqlWithDynamodb_fdw}/bin/initdb -E UTF8 -D /data
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
        postgresInitScriptPackage = pkgs.writeScriptBin "postgresInitScript" postgresInitScript;

        # It's been a bit of a pain to get the postgres user created during container creation... so let's just do it
        # in an init script that is run as root when the container starts up.  It creates the user and group, and then
        # creates the data dir, chowns it, and then runs the postgresInitScript as the postgres user.
        rootInitScript = ''
          #!${pkgs.runtimeShell}
          set -eu -o pipefail
          ${pkgs.dockerTools.shadowSetup}
          groupadd --system -g 999 postgres
          useradd --system --no-create-home -u 999 -g 999 postgres
          mkdir -p /data
          chown -R postgres:postgres /data
          ${pkgs.su}/bin/su postgres -c "${postgresInitScriptPackage}/bin/postgresInitScript"
        '';
        rootInitScriptPackage = pkgs.writeScriptBin "rootInitScript" rootInitScript;

      in {
        # "Test":
        #   nix build .#pythonWithDynamodb_fdw && ./result/bin/python -c "from dynamodbfdw import dynamodbfdw; dynamodbfdw.DynamoFdw"
        pythonWithDynamodb_fdw = pythonWithDynamodb_fdw;

        # "Test":
        #   nix build .#docker && podman load -i result -q && podman run --rm -it -p 127.0.0.1:5432:5432 --name dynamodb_fdw -v $HOME/.aws:/home/postgres/.aws ghcr.io/mfenniak/dynamodb_fdw:9.9
        docker = pkgs.dockerTools.buildLayeredImage {
          name = "ghcr.io/mfenniak/dynamodb_fdw";
          tag = fdwVersion;

          contents = [
            pkgs.bash
            pkgs.coreutils
            pythonWithDynamodb_fdw
            postgresqlWithDynamodb_fdw
            # If debug tooling is needed:
            # pkgs.gdb
            # pkgs.procps
            # pkgs.findutils
            # pkgs.gnugrep
          ];

          config = {
            Cmd = [
              "${rootInitScriptPackage}/bin/rootInitScript"
            ];
          };
        };
      };
    });
}

# To remotely debug the container with gdb:
#
# Review all the debug tooling commented out things above, like debug symbols and gdb, and uncomment them.
#
# (Run postgres in one terminal)
# nix build .#docker && podman load -i result -q && podman run --rm -it -p 127.0.0.1:5432:5432 -p 127.0.0.1:9999:9999 --name dynamodb_fdw -v $HOME/.aws:/home/postgres/.aws ghcr.io/mfenniak/dynamodb_fdw:9.9
#
# (in another terminal, run gdbserver)
# podman exec -it dynamodb_fdw /bin/bash
# (ps to search for connection)
# gdbserver :9999 --attach 21
#
# (in another terminal, attach gdb)
# gdb -q
# target remote localhost:9999
# cont
#
# Then in yet another terminal, do whatever you need to do to trigger the segfault.

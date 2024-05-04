{
  description = "yycpathways nix development environment";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};
      # nodejs = pkgs.nodejs_20;
      # nodeDependencies =
      #   (pkgs.callPackage ./node2nix { inherit pkgs system nodejs; })
      #   .nodeDependencies
      #   # This is a hack replacement of `src`
      #   # https://github.com/svanderburg/node2nix/issues/301
      #   .override (attrs: {
      #     src = builtins.path {
      #       name = "yycpathways-package-json-src";
      #       path = ./.;
      #       filter = f: _: builtins.elem (builtins.baseNameOf f) [
      #         "package.json"
      #         "package-lock.json"
      #       ];
      #     };
      #   });
    in {
      devShells.default =
        pkgs.mkShell {
          buildInputs = [ ];
          packages = [
            (pkgs.python3.withPackages (python-pkgs: [
              # select Python packages here
              python-pkgs.boto3
              python-pkgs.simplejson
            ]))
            # nodejs
            # pkgs.podman-compose
            # pkgs.node2nix
          ];
        };

      # devShells.codebuild =
      #   pkgs.mkShell {
      #     buildInputs = [ ];
      #     packages = [
      #       nodejs
      #       pkgs.awscli2
      #       pkgs.gnused
      #       pkgs.docker
      #       pkgs.jq
      #       pkgs.curl
      #       pkgs.postgresql
      #       pkgs.tailscale
      #       pkgs.node2nix
      #     ];
      #   };

      # /*
      # Refresh whenever package.json / package-lock.json changes...

      # pushd node2nix
      # node2nix -i ../package.json -l ../package-lock.json -18 -d
      # popd
      # */

      # packages = let
      #   yycpathwaysVersion = "9.9";
      #   appPackage = pkgs.stdenv.mkDerivation {
      #     name = "yycpathways-app";

      #     # This is a pretty hacky way to create the `src` for this package...
      #     # ideally it would be something like this:
      #     #   src = pkgs.lib.cleanSource (pkgs.nix-gitignore.gitignoreSource [ ./.gitignore ./.nixignore ] ./.);
      #     # which would not rebuild on every *.nix change and ignored file change.  But it seems that `./.`
      #     # causes the entire source path to still cause changes, so, even a whitespace change in flake.nix will
      #     # cause a rebuild.
      #     #
      #     # https://github.com/svanderburg/node2nix/issues/301 seems to address a similar problem in node2nix;
      #     # maybe after this is fixed (or the linked Discourse has a good resolution) we could use one of those.
      #     #
      #     # Well, this will do for now...
      #     src = [
      #       ./public
      #       ./src
      #       ./package.json
      #       ./package-lock.json
      #       ./tsconfig.json
      #       ./webpack.common.js
      #       ./webpack.prod.js
      #     ];
      #     unpackPhase = ''
      #       for srcFile in $src; do
      #         echo "cp $srcFile ..."
      #         cp -r $srcFile $(stripHash $srcFile)
      #       done
      #     '';

      #     buildInputs = [ nodejs ];

      #     # Two weird things in this buildPhase... the setting of HOME, and --loglevel=verbose...
      #     # both are to try to prevents writes to $HOME/.npm for npm logs, which in the CodeBuild container
      #     # ghcr.io/jmgilman/nix-aws-codebuild:0.1.0 causes permission errors.
      #     buildPhase = ''
      #       export HOME=$(pwd)
      #       export PATH="${nodeDependencies}/bin:$PATH"
      #       ln -s ${nodeDependencies}/lib/node_modules ./node_modules
      #       npm run build --loglevel=verbose
      #       npm run webpack-prod --loglevel=verbose
      #     '';
      #     installPhase = ''
      #       mkdir -p $out/app
      #       cp -r dist $out/app
      #       ln -s ${nodeDependencies}/lib/node_modules $out/app/node_modules
      #     '';
      #   };
      # in
      # {
      #   app = appPackage;

      #   docker = pkgs.dockerTools.buildLayeredImage {
      #     name = "docker.kainnef.com/yycbike";
      #     tag = yycpathwaysVersion;

      #     contents = [
      #       nodejs
      #       pkgs.coreutils
      #       appPackage
      #       pkgs.cacert
      #     ];

      #     # nix build .#docker && podman load -i result -q && podman run -it docker.kainnef.com/yycbike:9.9 strace node -e "require('axios').get('https://www.strava.com/').then(() => console.log('OK'), (err) => console.error(err));"

      #     config = {
      #       WorkingDir = "${appPackage}/app/dist/";
      #       Cmd = [
      #         "${nodejs}/bin/node"
      #         "-r" "${appPackage}/app/dist/instrumentation.js"
      #         "${appPackage}/app/dist/local.js"
      #       ];
      #       Env = [
      #         "ENABLE_SENTRY=yes"
      #         "YYCPATHWAYS_VERSION=${yycpathwaysVersion}"
      #         "NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-bundle.crt"
      #       ];
      #     };
      #   };
      # };
    });
}

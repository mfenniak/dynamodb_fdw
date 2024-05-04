{ stdenv, fetchFromGitHub, postgresql, python3, postgresqlTestHook }:

let
  multicorn = stdenv.mkDerivation rec {
    pname = "multicorn2";
    version = "v2.5";
    src = fetchFromGitHub {
      owner = "pgsql-io";
      repo = pname;
      rev = version;
      sha256 = "sha256-4fJ79zZIJbpTya/px4FG3tWnedQF5/0hlaJX+6BWcls=";
    };
    buildInputs = postgresql.buildInputs ++ [ postgresql python3 ];
    installPhase = ''
      runHook preInstall
      install -D multicorn${postgresql.dlSuffix} -t $out/lib/
      install -D sql/multicorn--''${version#v}.sql -t $out/share/postgresql/extension
      install -D multicorn.control -t $out/share/postgresql/extension
      runHook postInstall
    '';
  };

  multicornTest = stdenv.mkDerivation {
    name = "multicorn-test";
    dontUnpack = true;
    doCheck = true;
    buildInputs = [ postgresqlTestHook ];
    nativeCheckInputs = [ (postgresql.withPackages (ps: [ multicorn ])) ];
    postgresqlTestUserOptions = "LOGIN SUPERUSER";
    failureHook = "postgresqlStop";
    checkPhase = ''
      runHook preCheck
      psql -a -v ON_ERROR_STOP=1 -c "CREATE EXTENSION multicorn;"
      runHook postCheck
    '';
    installPhase = "touch $out";
  };

in {
  # nix-build -E 'with import <nixpkgs> {}; callPackage ./multicorn2.nix {}' -A package
  package = multicorn;

  # nix-build -E 'with import <nixpkgs> {}; callPackage ./multicorn2.nix {}' -A tests.extension
  tests = {
    extension = multicornTest;
  };
}
